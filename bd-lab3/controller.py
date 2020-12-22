import model
import view


def selection(first=None, *args):
    try:
        if first:
            first(*args)
        case = input()
        return case
    except:
        return -1


def enter_col_values(cnames, ctypes, first=None, empty_check=True):
    try:
        if first:
            first()
        res = []
        for cn, ct in zip(cnames, ctypes):
            sel_tmp = selection(view.col_input, cn, ct)
            if not empty_check or (empty_check and sel_tmp != ''):
                res.append((cn, sel_tmp))
        return res
    except:
        return []


def menu_level1():
    case1 = selection(view.section_level1)
    if case1 == '1':
        host = selection(view.s_host)
        port = selection(view.s_port)
        database = selection(view.s_database)
        user = selection(view.s_user)
        password = selection(view.s_password)
    elif case1 == '2':
        host, port, database, user, password = 'localhost', '5432', 'ElectronicsStore', 'postgres', '10'
    else:
        view.end()
        return
    db = model.sql(host, port, database, user, password)
    menu_level2(db)


def menu_level2(db):
    try:
        tables_lst = db.get_table_names()
        case2 = selection(view.section_level2, db.database, tables_lst)
        if case2 == str(len(tables_lst)+1):
            menu_level1()
        elif case2 in map(str, range(1, len(tables_lst)+1)):
            menu_level3(db, tables_lst[int(case2)-1])
        else:
            view.end()
            return
    except:
        menu_level1()


def menu_level3(db, table):
    try:
        case3 = selection(view.section_level3, table)
        if case3 == '1':
            db.select_all(table)
            selection()
        elif case3 == '2':
            col_names, col_types = db.get_column_names(table), db.get_column_types(table)
            db.insert(table, enter_col_values(col_names, col_types, view.header_section_data))
        elif case3 == '3':
            col_names, col_types = db.get_column_names(table), db.get_column_types(table)
            db.update(table, enter_col_values(col_names, col_types, view.header_section_data),
                      selection(view.section_where, False))
        elif case3 == '4':
            db.delete(table, selection(view.section_where, True))
        elif case3 == '5':
            db.gen_random(table, selection(view.section_gen_len))
        elif case3 == '6':
            try:
                col_names, col_types = db.get_column_names(table), db.get_column_types(table)
                sec_cols = [k for k in db.get_table_names() if k != table]
                table2 = sec_cols[int(selection(view.section_get_table2, sec_cols))-1]
                col_names2, col_types2 = db.get_column_names(table2), db.get_column_types(table2)
                rel1 = col_names[int(selection(view.section_join, table, col_names))-1]
                rel2 = col_names2[int(selection(view.section_join, table2, col_names2))-1]
                coln_conc, colt_conc = col_names+col_names2, col_types+col_types2
                search_coln, search_colt = list(zip(*[(coln_conc[int(x)-1], colt_conc[int(x)-1])\
                    for x in selection(view.section_columns, coln_conc).split()]))
                db.search_in_two_tables(table, table2, rel1, rel2, search_coln, enter_col_values(
                    search_coln, search_colt, view.header_section_search_two, empty_check=False),
                    enter_col_values(search_coln, search_colt, view.enter_values, empty_check=False))
                selection()
            except:
                pass
        elif case3 == '7':
            try:
                col_names, col_types = db.get_column_names(table), db.get_column_types(table)
                search_mode = int(selection(view.section_search_mode))
                if search_mode in [1, 2]:
                    text_col = col_names[int(selection(view.section_text_search_col, col_names))-1]
                    txt = selection(view.enter_text).split()
                    db.full_text_search(
                        table, text_col, txt, search_mode=search_mode)
                    selection()
            except:
                pass
        elif case3 == '8':
            menu_level2(db)
            return
        else:
            view.end()
            return
        menu_level3(db, table)
    except:
        menu_level2(db)
