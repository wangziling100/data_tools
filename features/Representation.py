import pandas as pd
import os
import numpy as np
from six.moves import xrange


class Representation:
    def __init__(self, keys, target, loader):
        """
        keys: key columns, with it one can identify the row, i.e. uid
        target: columns which to predict
        """
        self.keys = keys
        assert type(target) != list, 'only one target column can be set'
        self.target = target
        self.columns = {}
        self.table_nodes = {}
        self.loader = loader
        self.tables = []
        pass

    def check_data_type(self):
        df = self.load_main_tables(self.keys+[self.target])
        clazz = pd.unique(df[self.target]).tolist()
        n_class = len(clazz)
        self.is_balanced = True
        self.minority = []

        if n_class < 50 and df[self.target].dtype == 'int64':
            self.problem = 'classification'
            df_len = len(df)
            for c in clazz:
                if len(df[df[self.target] == c])/df_len > 0.8:
                    self.is_balanced = False
                    clazz.remove(c)
                    # minority is list
                    self.minority = clazz
                    # majority is value
                    self.majority = c

        else:
            self.problem = 'regression'
            # TODO

        return self.problem, self.is_balanced, self.minority, self.majority

    def _check_keys(self, columns):
        cnt = 0
        for key in self.keys:
            if key in columns:
                cnt += 1

        if cnt == len(self.keys):
            return True
        else:
            return False

    def _check_main_tables(self):
        # check whether all main tables have the same columns
        cnt = 0
        col_len = 0
        for node in self.table_nodes['root'].children:
            curr_col_len = len(self.columns[node.name]) 
            if curr_col_len != col_len:
                cnt += 1
                col_len = curr_col_len

        if cnt > 1:
            return False
        else:
            return True

    def _check_target(self, columns):
        if self.target in columns:
            return True
        else:
            return False

    def _find_common_keys(self, t1, t2):
        keys = []
        for col in self.columns[t1]:
            if col in self.columns[t2]:
                keys.append(col)

        return keys

    def _find_parent(self, t1):
        for node in self.table_nodes:
            t2 = self.table_nodes[node].name
            keys = self._find_common_keys(t1, t2)
            if len(keys) > 0:
                return True, keys, t2

        return False, [], None

    def create_table_tree(self):
        # create root
        self.table_nodes['root'] = table_node('root', [])
        self.columns['root'] = []
        t_tables = self.tables.copy()
        for i in xrange(100):
            li = []
            for t in t_tables:
                has_keys = False
                has_target = False

                if self._check_keys(self.columns[t]):
                    has_keys = True

                if self._check_target(self.columns[t]):
                    has_target = True

                if has_keys and has_target:
                    self.table_nodes[t] = table_node(
                            t, self.columns[t], self.table_nodes['root'])
                    li.append(t)
                    continue

                # find common keys and child table
                has_parent, keys, parent = self._find_parent(t)
                if has_parent:
                    self.table_nodes[t] = table_node(
                            t, self.columns[t], self.table_nodes[parent],
                            keys)
                    li.append(t)

            for t in li:
                t_tables.remove(t)

        assert len(t_tables) == 0, 'creation fails, there are still %d tables, that are in node transformitted' % len(t_tables)
        assert self._check_main_tables(), 'main tables have different columns'
    def get_all_cols(self):
        cols = []
        for t in self.columns:
            cols.extend(self.columns[t])

        cols = list(set(cols))
        return cols

    def get_all_cols_by_tables(self, tables):
        cols = []
        for table in tables:
            cols.extend(self.columns[table])

        cols = list(set(cols))
        return cols

    def get_cols_in_main_table(self):
        node = self.table_nodes['root'].children[0]
        return self.columns[node.name]

    def get_main_tables(self):
        ret = []
        for node in self.table_nodes['root'].children:
            ret.append(node.name)

        return ret

    def get_tables_by_cols(self, cols):
        ret = []
        for col in cols:
            for table in self.tables:
                if col in self.columns[table]:
                    ret.append(table)
        return list(set(ret))

    def get_tree_end(self, tables=None):
        # get the end of the given tree structure, maybe not leaf of the whole tree
        if tables is None:
            tree = list(self.table_nodes.values())
        else:
            tree = []
            tables = list(set(tables))
            for table in tables:
                assert table in self.table_nodes.keys(), 'unexpected table: '+table
                tree.append(self.table_nodes[table])

        # end is list of table name
        end = []
        not_end = []
        t_tree = tree.copy()
        for node in tree:
            t_tree.remove(node)
            for t_node in t_tree:
                if node.is_ancestor(t_node):
                    not_end.append(node.name)
            t_tree.append(node)

        not_end = list(set(not_end))

        for node in tree:
            if node.name not in not_end:
                end.append(node.name)

        return end

    def is_main_table(self, table):
        if self.table_nodes[table].parent.name == 'root':
            return True
        else:
            return False

    def load_main_tables(self, cols):
        assert len(self.table_nodes) != 0, 'generate table tree first'
        df = pd.DataFrame()
        
        # get path of the tables that has key and target
        for node in self.table_nodes['root'].children:
            curr_df = self.loader.load_table_with_cols(node.name, cols)
            df = pd.concat([df, curr_df])
        
        df = df.drop_duplicates()
        return df
        
    def generate_for_inbalanced_data(self):
        pass

    def load_column(self, path):
        fn = os.path.basename(path)
        self.tables.append(fn)
        self.columns[fn] = pd.read_csv(path, nrows=1).columns.tolist()

    def load_columns(self, paths):
        # paths: dict of paths
        for f in paths:
            self.tables.append(f)
            self.columns[f] = pd.read_csv(paths[f], nrows=1).columns.tolist()

    def load_table_with_cols(self, table, cols):
        cols = list(set(cols))
        t_cols = []
        for col in cols:
            if col in self.columns[table]:
                t_cols.append(col)
        return self.loader.load_table_with_cols(table, t_cols)

    def resample(self, tables=None, cols=None, problem=None, is_balanced=None, minority=None, majority=None, with_foreign_keys=False):
        """
        tables: which tables that want to be resampled, None means all talbes will be resampled
        cols:   which cols should be resample, needn't to know in which table, None means all columns will be resampled
        problme: classification or regression problem
        is_balanced: is it balanced data
        minority: if it is not balanced, which type value should be more resampled
        """
        if problem is None:
            problem = self.problem
        if is_balanced is None:
            is_balanced = self.is_balanced
        if minority is None:
            minority = self.minority
        if majority is None:
            majority = self.majority

        if cols is None and tables is None:
            tables = self.tables.copy()
            cols = self.get_all_cols()
        elif cols is None:
            cols = self.get_all_cols_by_tables(tables)
        elif tables is None:
            tables = self.get_tables_by_cols(cols)
        else:
            cols.extend(self.get_all_cols_by_tables(tables))
            tables.extend(self.get_tables_by_cols(cols))

        cols = list(set(cols))
        tables = list(set(tables))
        tables = self.get_tree_end(tables)
        print(tables)

        # check whether the columns in main table exist
        # keys and target will be always resampled
        t_cols = self.get_cols_in_main_table()
        main_cols = self.keys+[self.target]
        for col in cols:
            if col in t_cols:
                main_cols.append(col)
        # keys and target can appear twice, so unique them
        main_cols = list(set(main_cols))

        # load main table with these columns
        df = self.load_main_tables(main_cols)

        # resampled in these tables, it amouts to a base for join operation
        if problem == 'classification':
            if is_balanced:
                pass
            else:
                # minority will be all resample from main table
                resampled_minority = pd.DataFrame()
                for value in minority:
                    curr_df = df[df[self.target]==value]
                    resampled_minority = pd.concat([resampled_minority, curr_df])

                minority_len = len(resampled_minority)
                resampled_majority = df[df[self.target]==majority].sample(frac=1)[0:minority_len]
                pass

        # join the other columns in the other tables in it
        # TODO Bugs
        loaded_tables = []
        for table in tables:
            assert table in self.table_nodes.keys(), 'unexpected table: '+table
            # sometimes during loading child table, the parent table will be also loaded, shouldn't load it twice
            if table in loaded_tables:
                continue

            # load table
            t_cols = []
            t_tn = self.table_nodes[table]

            # main table has be loaded
            if t_tn.is_main_table():
                continue

            t_cols = cols + t_tn.keys
            curr_df = self.loader.load_table_with_cols(table, t_cols)

            # maybe this table has no direct foreign key with main table
            # so at first search ancester of this table util main table
            while not t_tn.parent.is_main_table():
                parent = t_tn.parent
                parent_cols = t_tn.keys+parent.keys+cols
                parent_df = self.load_table_with_cols(parent.name, parent_cols)
                curr_df = pd.merge(parent_df, curr_df, how='left', on=t_tn.keys)
                t_tn = parent
                loaded_tables.append(table)

            curr_df = curr_df.drop_duplicates()

            # merge
            # reduce duplicte columns
            cols1 = resampled_minority.columns
            if with_foreign_keys:
                cols2 = curr_df.columns
            else:
                cols2 = cols + self.keys
            cols2 = list(set(cols2))
            for col in cols1:
                if col in cols2:
                    cols2.remove(col)
            cols2.extend(self.keys)
            
            resampled_minority = pd.merge(resampled_minority, curr_df[cols2], how='left', on=self.keys)

            resampled_majority = pd.merge(resampled_majority, curr_df[cols2], how='left', on=self.keys)

            resampled_minority = resampled_minority.drop_duplicates()
            resampled_majority = resampled_majority.drop_duplicates()

        return resampled_minority, resampled_majority

    def show_columns(self):
        for col in self.columns:
            print('table: '+col)
            print(self.columns[col])
            print('\n')

    def show_table_tree(self):
        print('table tree:')
        print('\n')
        for name in self.table_nodes:
            node = self.table_nodes[name]
            print('children of '+name)
            for child in node.children:
                print(child.name)
            if name != 'root':
                print('parent of '+name+':'+node.parent.name)
                str_key = ''
                for k in node.keys:
                    str_key += k+' '
                print('keys: '+str_key)
            print('\n')

    def show_tables(self):
        print('tables:')
        print(self.tables)
        print('\n')

    def type_transformation(self, df, fillan=None, threshold=10):
        cols = df.columns
        for col in cols:
            if df[col].dtype == np.object:
                li = df[col].tolist()
                li = list(set(li))
                
                if len(li) > threshold:
                    try:
                        df[col] = df[col].astype('float')
                    except ValueError:
                        df[col] = df[col].fillna('nan').astype('category')
                else:
                    df[col] = df[col].fillna('nan').astype('category')
                    t_df = pd.get_dummies(df[col])
                    df = pd.concat([df, t_df],axis=1)
                    df = df.drop(columns=[col])

        return df.dropna()


class table_node:

    def __init__(self, name, cols, parent=None, keys=None):
        self.name = name
        # each node has only one of parent 
        self.parent = parent
        self.children = []
        self.columns = cols
        if parent is not None:
            parent.add_child(self)

            if keys is not None:
                self.keys = keys
            else:
                self.keys = self.find_keys(parent)

    def add_child(self, child):
        self.children.append(child)

    def del_col(self, col):
        assert col in self.columns, "the deleted column is not in it"
        self.columns.remove(col)

    def is_ancestor(self, node):
        if node.name == 'root':
            return False
        if self.name == 'root':
            return True
        ret = False
        t_tn = node
        while t_tn.parent.name != 'root':
            if self.name == t_tn.parent.name:
                return True
            t_tn = t_tn.parent

        return ret

    def is_main_table(self):
        if self.parent is None:
            return False
        if self.parent.name == 'root':
            return True
        else:
            return False

    def set_cols(self, columns):
        self.colunms = columns

    def find_keys(self, parent):
        # find out commen key between itself and its parent
        keys = []
        for col in self.columns:
            if col in parent.columns:
                keys.append(col)

        if parent.name != 'root':
            assert len(keys) != 0, "this table node has a wrong parent"
        return keys


class inbalance_data:
    def __init__(self):
        pass
