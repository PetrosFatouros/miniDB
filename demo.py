from order import *

# -->Create custom db (2 tables with 8 and 5 records each)
def create_custom_db():
    # -->Create database
    orderDB = Database('order', load=False)
    # -->Create table
    create_table_O(orderDB, 'superheroes', ['heroname', 'realname', 'age'], [str, str, int], primary_key='heroname', load=None,
                   file_organization_type='sequential')
    # -->Insert records
    insert_O(orderDB, 'superheroes', ['Iron_Man', 'Tony_Stark', 53], lock_load_save=True)
    insert_O(orderDB, 'superheroes', ['Batman', 'Bruce_Wayne', 26], lock_load_save=True)
    insert_O(orderDB, 'superheroes', ['Hulk', 'Bruce_Banner', 43], lock_load_save=True)
    insert_O(orderDB, 'superheroes', ['Spiderman', 'Peter_Parker', 28], lock_load_save=True)
    insert_O(orderDB, 'superheroes', ['Captain_America', 'Steve_Rogers', 66], lock_load_save=True)

    insert_O(orderDB, 'superheroes', ['Wolverine', 'James_Howlett', 197], lock_load_save=True)
    insert_O(orderDB, 'superheroes', ['Deadpool', 'Wade_Wilson', 50], lock_load_save=True)
    insert_O(orderDB, 'superheroes', ['Hawkeye', 'Clint_Barton', 38], lock_load_save=True)

    insert_O(orderDB, 'superheroes', ['Impostor', 'Clint_Barton', 38], lock_load_save=True)
    insert_O(orderDB, 'superheroes', ['Impostor', 'xxxxxxxxxxxx', 00], lock_load_save=True)

    # -->Create table
    create_table_O(orderDB, 'celebrities', ['fullname', 'profession'], [str, str], primary_key='fullname', load=None,
                    file_organization_type='heap')
    # -->Insert records
    insert_O(orderDB, 'celebrities', ['Tom_Cruise', 'movie_actor'], lock_load_save=True)
    insert_O(orderDB, 'celebrities', ['Ariana_Grande', 'pop_singer'], lock_load_save=True)
    insert_O(orderDB, 'celebrities', ['James_Charles', 'instagram_star'], lock_load_save=True)
    insert_O(orderDB, 'celebrities', ['Johnny_Depp', 'movie_actor'], lock_load_save=True)
    insert_O(orderDB, 'celebrities', ['Nicki_Minaj', 'rapper'], lock_load_save=True)
    #-->Completion message
    print('----Custom database created----\n')
    return orderDB

# -->Print useful Table's object information
def print_info(db_object):
    print()
    db_object.load(db_object.savedir)
    print(f'----Database "{db_object._name}"----')
    print(f'savedir: "{db_object.savedir}"')
    db_object.show_table('meta_insert_stack')
    for table in db_object.tables.values():
        if table._name[:4] != 'meta':
            print(f'--Table "{table._name}"--')
            print(f'_no_of_columns: {table._no_of_columns}')
            print(f'column_names: {table.column_names}')
            print(f'pk_idx: {table.pk_idx}')
            print(f'_file_organization_type: "{table._file_organization_type}"')
            print(f'columns: {table.columns}')
            print(f'data: {table.data}')
            if table._file_organization_type == 'sequential':
                print(f'sequential_file_columns: {table.sequential_file_columns}')
                print(f'sequential_file_data: {table.sequential_file_data}')
                print(f'order: {table.order}')
            print()

# -->Driver code
orderDB = create_custom_db()
print_info(orderDB)
