from database import Database
from table import Table
from misc import split_condition, get_op
import binary_search

'''====================----Create Table----===================='''
#-->Wrap Database method, add file_organization_type variable(str)
def create_table_O(db_object, name=None, column_names=None, column_types=None, primary_key=None, load=None,
                   file_organization_type='heap'):
    if file_organization_type == 'sequential':
        if primary_key is not None:
            # -->Call initial method
            db_object.create_table(name, column_names, column_types, primary_key, load)
            # -->Load from file
            db_object.load(db_object.savedir)
            # -->Same as 'columns' variable, but for the sequential main file
            sequential_file_columns = []
            for i in range(db_object.tables[name]._no_of_columns):
                sequential_file_columns.append([])
            # -->Same as 'data' variable, but for the sequential main file
            sequential_file_data = []
            # -->Holds the ordered position of each record [position, PK field data]
            order = []
            # -->Set attributes
            setattr(db_object.tables[name], 'sequential_file_columns', sequential_file_columns)
            setattr(db_object.tables[name], 'sequential_file_data', sequential_file_data)
            setattr(db_object.tables[name], 'order', order)
        else:
            print('Table was not created. Set a primary key in order to use sequential file organization.')
            pass
    elif file_organization_type == 'heap':
        # -->Call initial method
        db_object.create_table(name, column_names, column_types, primary_key, load)
    # -->Set attribute '_file_organization_type'
    setattr(db_object.tables[name], '_file_organization_type', file_organization_type)
    # -->Save to file
    db_object._update()
    db_object.save()
'''============================================================'''

'''====================----Insert----===================='''
# -->Wrap Database method, add file_organization_type variable(str)
def insert_O(db_object, table_name, row, lock_load_save=True):
    if db_object.tables[table_name]._file_organization_type == 'sequential':
        primary_key_field_data = row[db_object.tables[table_name].pk_idx]
        temp = [row[1] for row in db_object.tables[table_name].order]
        position, exists = binary_search._binary_search_v2(temp, primary_key_field_data)
        if exists == True:
            print(f'\n--- {primary_key_field_data} exists. Record was not inserted.')
        else:
            # -->Call initial method
            db_object.insert(table_name, row, lock_load_save)
            # -->Load from file
            db_object.load(db_object.savedir)
            # -->Update variable 'order'
            db_object = _update_order(db_object, table_name, primary_key_field_data)
            # -->Count the non-None records in the insert stack
            insert_stack_records = 0
            for record in db_object.tables[table_name].data:
                #-->If primary key field data is None then the record was deleted
                if record[db_object.tables[table_name].pk_idx] is not None:
                    insert_stack_records += 1
            #-->Merge insert stack with main file if the limit is reached
            limit = 5
            if insert_stack_records == limit:
                db_object = _incorporate_insert_stack(db_object, table_name)
    else:
        # -->Call initial method
        db_object.insert(table_name, row, lock_load_save)
        # -->Load from file
        db_object.load(db_object.savedir)
    # -->Save to file
    db_object._update()
    db_object.save()

# -->Find record's corresponding position and update 'order' variable
def _update_order(db_object, table_name, primary_key_field_data):
    temp = [row[1] for row in db_object.tables[table_name].order]
    position, exists = binary_search._binary_search_v2(temp, primary_key_field_data)
    #-->Fix indexes
    for elem in db_object.tables[table_name].order:
        if elem[0] >= position:
            elem[0] += 1
    # -->Update 'order' variable
    if exists == True:
        pass
    else:
        db_object.tables[table_name].order.insert(position, [position, primary_key_field_data])
    return db_object

# -->Incorporate insert stack with main file
def _incorporate_insert_stack(db_object, table_name):
    data = db_object.tables[table_name].data
    seq_data = db_object.tables[table_name].sequential_file_data
    order = db_object.tables[table_name].order
    # -->Incorporate 'data' with 'sequential_file_data'
    for record in data:
        seq_data.append(record)
    seq_data.sort(key=lambda x: x[0])
    # -->Incorporate 'columns' with 'sequential_file_columns' (based on 'sequential_file_data')
    seq_columns = [[row[i] for row in seq_data] for i in range(db_object.tables[table_name]._no_of_columns)]
    db_object.tables[table_name].sequential_file_columns = seq_columns
    # -->Save to file
    db_object._update()
    db_object.save()
    # -->Delete records from insert new_stack
    for item in order:
        db_object.delete(table_name, f'heroname=={item[1]}')
        # -->Load from file
        db_object.load(db_object.savedir)
    return db_object
'''======================================================'''

'''====================----Select----===================='''
 # -->Database method, distinguish select method for each file organization type
def select_O(db_object, table_name, columns, condition=None, order_by=None, asc=False, top_k=None, save_as=None,
             return_object=False):
    # -->Load from file
    db_object.load(db_object.savedir)
    # -->Check if table is locked. If not, then lock it exclusively and proceed
    if db_object.is_locked(table_name):
        return
    db_object.lockX_table(table_name)
    if db_object.tables[table_name]._file_organization_type == 'heap':
        # -->Call initial method
        table = db_object.tables[table_name]._select_where(columns, condition, order_by, asc, top_k)
    elif db_object.tables[table_name]._file_organization_type == 'sequential':
        # -->Table with selected columns and records
        table = _select_where_O(db_object.tables[table_name], columns, condition, order_by, asc, top_k)
    # -->Unlock table
    db_object.unlock_table(table_name)
    # -->Return result table (save to file/ return object/ display table)
    if save_as is not None:
        table._name = save_as
        db_object.table_from_object(table)
    else:
        if return_object:
            return table
        else:
            table.show()

# -->Corresponding '_select_where' Table method
def _select_where_O(tb_object, return_columns, condition=None, order_by=None, asc=False, top_k=None):
    #-->Select all columns
    if return_columns == '*':
        return_cols = [i for i in range(len(tb_object.column_names))]
    #-->Invalid parameter input
    elif isinstance(return_columns, str):
        raise Exception(
            f'Return columns should be "*" or of type list. (the second parameter is return_columns not condition)')
    #-->Select only specified columns
    else:
        return_cols = [tb_object.column_names.index(colname) for colname in return_columns]

    #-->Return records based on condition
    select_data = []
    if condition is not None:
        column_name, operator, value = tb_object._parse_condition(condition)
        #-->Condition's culumn index
        column = tb_object.column_names.index(column_name)

        #-->Check if column_name is the primary key column
        if column_name == tb_object.column_names[tb_object.pk_idx]: #-->PK

            if operator == '==': #-->PK-identity
                #-->Check if record exists in main file
                temp = [row[tb_object.pk_idx] for row in tb_object.sequential_file_data]
                position = binary_search._binary_search(temp, value)
                #-->Record exists in main file
                if position != -1:
                    select_data.append(tb_object.sequential_file_data[position])
                # -->Search in insert stack
                else:
                    if value in [row[tb_object.pk_idx] for row in tb_object.data]:
                        for record in tb_object.data:
                            if get_op('==', record[tb_object.pk_idx], value):
                                select_data.append(record)

            else: #-->PK-range
                temp = [row[1] for row in tb_object.order]
                position, exists = binary_search._binary_search_v2(temp, value)
                #-->Get the starting position (lower limit)
                if operator == "<" or operator == "<=":
                    lower_limit = 0
                elif operator == ">" and exists == True:
                    lower_limit = position + 1
                else:
                    lower_limit = position

                #-->Get corresponding records from both main file and insert stack
                i = lower_limit
                while i < len(temp) and get_op(operator, temp[i], value):
                    #-->Record exists in main file
                    if temp[i] in tb_object.sequential_file_columns[column]:
                        select_data.append(tb_object.sequential_file_data[tb_object.sequential_file_columns[column].index(temp[i])])
                    #-->Record exists in insert stack
                    elif temp[i] in tb_object.columns[column]:
                        select_data.append(tb_object.data[tb_object.columns[column].index(temp[i])])
                    i += 1

        else: #-->Not PK
            #-->Get corresponding records from both main file and insert stack
            for elem in tb_object.sequential_file_data:
                if get_op(operator, elem[column], value):
                    select_data.append(elem)
            for elem in tb_object.data:
                if get_op(operator, elem[column], value):
                    select_data.append(elem)

    #-->Return all records
    else:
        #-->Get all records from both main file and insert stack
        for elem in tb_object.sequential_file_data:
            select_data.append(elem)
        for elem in tb_object.data:
            select_data.append(elem)

    select_columns = [[row[i] for row in select_data] for i in range(tb_object._no_of_columns)]
    #-->Selected records indexes
    rows = [i for i in range(len(select_columns[0]))]
    #-->Select only top k records
    rows = rows[:top_k]
    #-->Store result table's information in a dictionary
    dict = {(key): ([[select_data[i][j] for j in return_cols] for i in rows] if key == "data" else value) for key, value
            in tb_object.__dict__.items()}
    dict['column_names'] = [tb_object.column_names[i] for i in return_cols]
    dict['column_types'] = [tb_object.column_types[i] for i in return_cols]
    dict['_no_of_columns'] = len(return_cols)
    #-->Return result table
    if order_by is None:
        return Table(load=dict)
    #-->Return result table ordered
    else:
        return Table(load=dict).order_by(order_by, asc)
'''======================================================'''
