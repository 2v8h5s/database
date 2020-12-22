import sqlalchemy as sqlal
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import select
from sqlalchemy.sql import text
import view
import controller


class sql:
    def __init__(self, host, port, database, user, password):
        self.host, self.port, self.database, self.user, self.password = host, port, database, user, password

    def connect(self):
        try:
            self.engine = sqlal.create_engine('postgresql://'+self.user+':' +
                                              self.password+'@'+self.host+':'+self.port+'/'+self.database)
            self.meta = sqlal.MetaData()
            self.meta.reflect(self.engine)
            self.base = automap_base(metadata=self.meta)
            self.base.prepare()
            self.session = Session(self.engine)
        except Exception as e:
            view.error(e)
            controller.selection()
    
    def close(self):
        try:
            self.session.close()
        except Exception as e:
            pass
    
    def get_table_names(self):
        try:
            self.connect()
            res = list(self.meta.tables.keys())
            self.close()
            return res
        except Exception as e:
            pass            

    def get_column_names(self, table):
        try:
            self.connect()
            res = [c.name for c in self.meta.tables[table].columns]
            self.close()
            return res
        except Exception as e:
            pass

    def get_column_types(self, table):
        try:
            self.connect()
            res = [str(c.type) for c in self.meta.tables[table].columns]
            self.close()
            return res
        except Exception as e:
            pass

    def get_pkeys(self, table):
        try:
            self.connect()
            res = [c.name for c in self.meta.tables[table].columns if c.primary_key==True]
            self.close()
            return res
        except Exception as e:
            pass

    def get_fkeys(self, table):
        try:
            self.connect()
            res = [(c.name, *(str(list(self.meta.tables[table].columns[c.name].foreign_keys)[0].column).split('.')))
                    for c in self.meta.tables[table].columns if c.foreign_keys]
            self.close()
            return res
        except Exception as e:
            pass

    def get_col_info(self, table):
        return self.get_column_names(table), self.get_column_types(table), \
            self.get_pkeys(table), self.get_fkeys(table), 

    def select_all(self, table):
        col_names, col_types, pkeys, fkeys = self.get_col_info(table)
        try:
            self.connect()
            qr = self.session.query(self.base.classes[table]).limit(1000)
            data = [[getattr(q, c) for c in col_names] for q in qr]
            view.print_table(col_names, col_types, pkeys, fkeys, data)
        except Exception as e:
            view.error(e)
            controller.selection()
        finally:
            self.close()

    def insert(self, table, sels):
        try:
            self.connect()
            self.session.add(self.base.classes[table](**dict(sels)))
            self.session.commit()
        except Exception as e:
            view.error(e)
            controller.selection()
        finally:
            self.close()
        
    def update(self, table, sels, where):
        try:
            self.connect()
            qr = self.session.query(self.base.classes[table]).filter(text(where)).all()
            for tq in qr:
                for cs,cv in sels:
                    setattr(tq, cs, cv)
            self.session.commit()
        except Exception as e:
            view.error(e)
            controller.selection()
        finally:
            self.close()

    def delete(self, table, where):
        try:
            self.connect() 
            qr = self.session.query(
                self.base.classes[table]).filter(text(where)).all()
            for tq in qr:
                self.session.delete(tq)
            self.session.commit()
        except Exception as e:
            view.error(e)
            controller.selection()
        finally:
            self.close()
    
    def gen_random(self, table, gen_len):
        col_names, col_types, pkeys, fkeys = self.get_col_info(table)
        self.delete(table, '')
        try:
            self.connect()
            self.session.execute(text("INSERT INTO public.\"{}\" (SELECT {} FROM generate_series(1, {}));"
                .format(table, ", ".join(["(SELECT {} FROM public.\"{}\" ORDER BY RANDOM()+generate_series LIMIT 1)"
                    .format([f[2] for f in fkeys if f[0] == c][0],
                        [f[1] for f in fkeys if f[0] == c][0])
                            if c in [f[0] for f in fkeys] else (("generate_series" if 'INTEGER' in t or 'NUMERIC' in t else "generate_series::text")
                                if c in pkeys else (("NOW()+(random()*(interval '90 days'))+'30 days'" if 'TIMESTAMP' in t 
                                    else ("cast(random()::int as boolean)" if t == 'BOOLEAN'
                                        else ("SUBSTRING(md5(random()::text),1,5)" if 'TEXT' in t or 'VARCHAR' in t
                                            else "(random()*1000000)::int")))))
                                                for c, t in zip(col_names, col_types)]), gen_len)))
            self.session.commit()
        except Exception as e:
            view.error(e)
            controller.selection()
        finally:
            self.close()

    def search_in_two_tables(self, table, table2, rel1, rel2, search_col, mods, values):
        col_names, col_types, pkeys, fkeys = self.get_col_info(table)
        col_names2, col_types2, pkeys2, fkeys2 = self.get_col_info(table2)
        try:
            self.connect()
            data = self.session.execute(
                text("SELECT * FROM public.\"{0}\" JOIN public.\"{1}\" ON public.\"{0}\".{2} = public.\"{1}\".{3} WHERE {4};"
                .format(table, table2, rel1, rel2,
                        " AND ".join([(c+"=:"+c if m[1] == '' else "("+c+" BETWEEN "+v[1].split()[0]+" AND "+v[1].split()[1]+")")
                                                        for c, m, v in zip(search_col, mods, values)]))),
                dict([(c, v[1]) for c, m, v in zip(search_col, mods, values) if m[1] != 'r'])).fetchall()
            view.print_table(col_names+col_names2, col_types+col_types2, pkeys+pkeys2, fkeys+fkeys2, data)
        except Exception as e:
            view.error(e)
        finally:
            self.close()

    def full_text_search(self, table, text_col, txt, search_mode=1):
        col_names, col_types, pkeys, fkeys = self.get_col_info(table)
        try:
            self.connect()
            data = self.session.execute(text("SELECT * FROM public.\"{}\" WHERE {} (to_tsvector({}) @@ to_tsquery('{}'));"
                .format(table, ("NOT" if search_mode == 1 else ""),
                        text_col, ('|' if search_mode == 1 else '&').join(txt)))).fetchall()
            view.print_table(col_names, col_types, pkeys, fkeys, data)
        except Exception as e:
            view.error(e)
        finally:
            self.close()
