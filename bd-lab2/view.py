import os
from terminaltables import AsciiTable


def section_level1(*args):
    os.system('cls')
    print('Select action or any other key to quit:')
    print('\t1. Define database details manually')
    print('\t2. Use defaults')


def section_level2(*args):
    os.system('cls')
    print('Select table from "%s" database or any other key to quit: ' % (args[0]))
    for ind, row in enumerate(args[1]):
        print('\t'+str(ind+1)+'. '+row)
    print('\t'+str(len(args[1])+1)+'. return back')


def section_level3(*args):
    os.system('cls')
    print('Select action for "%s" table or any other key to quit:' % (args[0]))
    print('\t1. Select all')
    print('\t2. Insert')
    print('\t3. Update')
    print('\t4. Delete')
    print('\t5. Generate random data')
    print('\t6. Search in two tables')
    print('\t7. return back')


def section_input(*args):
    print('\t'+args[0]+': ', end='')


def col_input(*args):
    print('\t'+args[0]+' ('+args[1]+'): ', end='')


def section_where(*args):
    if args[0]:
        os.system('cls')
    print('WHERE ', end='')


def section_gen_len(*args):
    os.system('cls')
    print('Enter number of rows to generate: ', end='')


def section_get_table2(*args):
    os.system('cls')
    print('Select second table name or any other key to return back: ')
    for ind, row in enumerate(args[0]):
        print('\t'+str(ind+1)+'. '+''.join(row))


def section_join(*args):
    print('Select join column for "%s" table or any other key to return back: '%args[0])
    for ind, ct in enumerate(args[1]):
        print('\t'+str(ind+1)+'. '+ct)


def section_columns(*args):
    print('Select columns to search in or any other key to return back: ')
    for ind, row in enumerate(args[0]):
        print('\t'+str(ind+1)+'. '+''.join(row))


def section_text_search_col(*args):
    print('Select column to search in or any other key to return back: ')
    for ind, row in enumerate(args[0]):
        print('\t'+str(ind+1)+'. '+''.join(row))


def section_search_mode(*args):
    os.system('cls')
    print('Enter search mode (\'1\'-not word match, \'2\'-word match): ', end='')


def enter_text(*args):
    print('Enter words to search: ', end='')


def header_section_data():
    os.system('cls')
    print('Enter data or leave empty to skip a column:')


def header_section_search_two():
    print('Enter conditions types (\'r\'-range or Enter if enumeration):')


def enter_values():
    print('Enter values: ')


def s_host(*args):
    os.system('cls')
    print('Host: ', end='')


def s_port(*args):
    os.system('cls')
    print('Port: ', end='')


def s_database(*args):
    os.system('cls')
    print('DataBase: ', end='')
    

def s_user(*args):
    os.system('cls')
    print('User: ', end='')


def s_password(*args):
    os.system('cls')
    print('Password: ', end='')


def error(e):
    print('[ERROR]', str(e))


def print_table(cnames, ctypes, pkeys, fkeys, data):
    os.system('cls')
    try:
        print(AsciiTable([[cn+'\n('+ct+(')-PK-FK' if cn in pkeys and cn in [f[0] for f in fkeys] else (')-PK' if cn in pkeys else (
            ')-FK' if cn in [f[0] for f in fkeys] else ')'))) for (cn, ct) in zip(cnames, ctypes)]]+data).table)
        print('Press Enter to continue...')
    except:
        pass


def end():
    print('Closing')
