import psycopg2 as ps2
import psycopg2.extras
import psycopg2.sql
import view
import controller


class sql:
    def __init__(self, host, port, database, user, password):
        self.host, self.port, self.database, self.user, self.password = host, port, database, user, password

    def connect(self):
        try:
            self.cn = ps2.connect(host=self.host, port=self.port,
                                  database=self.database, user=self.user, password=self.password)
            self.cr = self.cn.cursor(cursor_factory=psycopg2.extras.DictCursor)            
        except (Exception, ps2.Error) as e:
            view.error(e)
            controller.selection()
    
    def close(self):
        try:
            self.cr.close()
            self.cn.close()
        except (Exception, ps2.Error) as e:
            pass
    
    def get_table_names(self):
        try:
            self.connect()
            self.cr.execute(psycopg2.sql.SQL(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"))
            res = [v[0] for v in self.cr.fetchall()]
            self.close()
            return res
        except (Exception, ps2.Error) as e:
            pass
       
    def get_column_names(self, table):
        try:
            self.connect()
            self.cr.execute(psycopg2.sql.SQL("SELECT column_name FROM information_schema.columns WHERE \
                    (table_schema='public' and table_name= %s) ORDER BY ordinal_position;"), (table,))
            res = [v[0] for v in self.cr.fetchall()]
            self.close()
            return res
        except (Exception, ps2.Error) as e:
            pass
      
    def get_column_types(self, table):
        try:
            self.connect()
            self.cr.execute(psycopg2.sql.SQL("SELECT data_type FROM information_schema.columns WHERE \
                    (table_schema='public' and table_name= %s) ORDER BY ordinal_position;"), (table,))
            res = [v[0] for v in self.cr.fetchall()]
            self.close()
            return res
        except (Exception, ps2.Error) as e:
            pass

    def get_pkeys(self, table):
        try:
            self.connect()
            self.cr.execute(psycopg2.sql.SQL("SELECT kcu.column_name\
                from information_schema.table_constraints tco\
                join information_schema.key_column_usage kcu on kcu.constraint_name=tco.constraint_name\
                and kcu.constraint_schema=tco.constraint_schema and kcu.constraint_name=tco.constraint_name\
                where tco.constraint_type='PRIMARY KEY' and kcu.table_name='{}'").format(psycopg2.sql.SQL(table)))
            res = [v[0] for v in self.cr.fetchall()]
            self.close()
            return res
        except (Exception, ps2.Error) as e:
            pass
      
    def get_fkeys(self, table):
        try:
            self.connect()
            self.cr.execute(psycopg2.sql.SQL("SELECT stb.column_name, ftb.table_name, ftb.column_name\
                from information_schema.key_column_usage ftb join\
                (SELECT tco.column_name, tco.constraint_name, rct.unique_constraint_name\
                from information_schema.key_column_usage tco join information_schema.referential_constraints\
                rct on tco.constraint_name=rct.constraint_name where tco.table_name='{}') stb\
                on ftb.constraint_name=stb.unique_constraint_name").format(psycopg2.sql.SQL(table)))
            res = self.cr.fetchall()
            self.close()
            return res
        except (Exception, ps2.Error) as e:
            pass
    
    def get_col_info(self, table):
        return self.get_column_names(table), self.get_column_types(table), \
            self.get_pkeys(table), self.get_fkeys(table), 

    def select_all(self, table):
        col_names, col_types, pkeys, fkeys = self.get_col_info(table)
        try:
            self.connect()
            self.cr.execute(
                psycopg2.sql.SQL("SELECT * FROM public.{};")
                .format(psycopg2.sql.Identifier(table)))
            data = self.cr.fetchall()
            view.print_table(col_names, col_types, pkeys, fkeys, data)
        except (Exception, ps2.Error) as e:
            view.error(e)
            controller.selection()
        finally:
            self.close()

    def insert(self, table, sels):
        try:
            self.connect()
            self.cr.execute(psycopg2.sql.SQL("INSERT INTO public.{} ({}) VALUES ({});")
                            .format(psycopg2.sql.Identifier(table),
                                    psycopg2.sql.SQL(
                                        ", ".join([s[0] for s in sels])),
                                    psycopg2.sql.SQL(", ".join(["%s" for i in range(len(sels))]))),
                            tuple([s[1] for s in sels]))
            self.cn.commit()
        except (Exception, ps2.Error) as e:
            view.error(e)
            controller.selection()
        finally:
            self.close()
        
    def update(self, table, sels, where):
        try:
            self.connect()
            self.cr.execute(psycopg2.sql.SQL("UPDATE public.{} SET {} {};")
                            .format(psycopg2.sql.Identifier(table),
                                    psycopg2.sql.SQL(
                                        ", ".join([s[0]+"=%s" for s in sels])),
                                    (psycopg2.sql.SQL("WHERE "+where) if where else psycopg2.sql.SQL(""))),
                            tuple([s[1] for s in sels]))
            self.cn.commit()
        except (Exception, ps2.Error) as e:
            view.error(e)
            controller.selection()
        finally:
            self.close()

    def delete(self, table, where):
        try:
            self.connect()
            self.cr.execute(psycopg2.sql.SQL("DELETE FROM {} {};")
                            .format(psycopg2.sql.Identifier(table),
                                    (psycopg2.sql.SQL("WHERE "+where) if where else psycopg2.sql.SQL(""))))
            self.cn.commit()
        except (Exception, ps2.Error) as e:
            view.error(e)
            controller.selection()
        finally:
            self.close()
    
    def gen_random(self, table, gen_len):
        col_names, col_types, pkeys, fkeys = self.get_col_info(table)
        self.delete(table, '')
        try:
            self.connect()
            self.cr.execute(psycopg2.sql.SQL(
                "INSERT INTO public.{} (SELECT {} FROM generate_series(1, {}));")
                .format(psycopg2.sql.Identifier(table),
                    psycopg2.sql.SQL(", ".join([(psycopg2.sql.SQL("(SELECT {} FROM public.{} ORDER BY RANDOM()+generate_series LIMIT 1)")
                        .format(psycopg2.sql.SQL([f[2] for f in fkeys if f[0] == c][0]),
                            psycopg2.sql.Identifier([f[1] for f in fkeys if f[0] == c][0])).as_string(self.cn)
                                if c in [f[0] for f in fkeys] else (("generate_series" if t in ['integer', 'numeric'] else "generate_series::text")
                                    if c in pkeys else (("NOW()+(random()*(interval '90 days'))+'30 days'" if 'timestamp' in t 
                                        else ("cast(random()::int as boolean)" if t == 'boolean'
                                            else ("SUBSTRING(md5(random()::text),1,5)" if t in ['text', 'character varying']
                                                else "(random()*1000)::int"))))))
                                                    for c, t in zip(col_names, col_types)])), psycopg2.sql.SQL(gen_len)))
            self.cn.commit()
        except (Exception, ps2.Error) as e:
            view.error(e)
            controller.selection()
        finally:
            self.close()

    def search_in_two_tables(self, table, table2, rel1, rel2, search_col, mods, values):
        col_names, col_types, pkeys, fkeys = self.get_col_info(table)
        col_names2, col_types2, pkeys2, fkeys2 = self.get_col_info(table2)
        try:
            self.connect()
            self.cr.execute(
                psycopg2.sql.SQL(
                    "SELECT * FROM public.{0} JOIN public.{1} ON public.{0}.{2} = public.{1}.{3} WHERE {4};")
                .format(psycopg2.sql.Identifier(table), psycopg2.sql.Identifier(table2), psycopg2.sql.SQL(rel1), psycopg2.sql.SQL(rel2),
                        psycopg2.sql.SQL(" AND ".join([(c+"=%s" if m[1] == '' else "("+c+" BETWEEN "+v[1].split()[0]+" AND "+v[1].split()[1]+")")
                                                        for c, m, v in zip(search_col, mods, values)]))),
                tuple([v[1] for m, v in zip(mods, values) if m[1] != 'r']))
            data = self.cr.fetchall()
            view.print_table(col_names+col_names2, col_types+col_types2, pkeys+pkeys2, fkeys+fkeys2, data)
        except (Exception, ps2.Error) as e:
            view.error(e)
        finally:
            self.close()
