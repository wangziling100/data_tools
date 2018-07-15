import pandas as pd
import os
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
        self.tables = []
        self.columns = {}
        self.table_nodes = {}
        self.loader = loader
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
                    self.minority = clazz

        else:
            self.problem = 'regression'
            # TODO

        return self.problem, self.is_balanced, self.minority

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
        t_tables = self.tables
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

    def load_main_tables(self, cols):
        assert len(self.table_nodes) != 0, 'generate table tree first'
        df = pd.DataFrame()
        
        # get path of the tables that has key and target
        for node in self.table_nodes['root'].children:
            curr_df = self.loader.load_table_with_cols(node.name, cols)
            df = pd.concat([df, curr_df])
        
        df = df.drop_duplicates()
        return df
        
    # TODO def generate_data(self):
    def load_column(self, path):
        fn = os.path.basename(path)
        self.tables.append(fn)
        self.columns[fn] = pd.read_csv(path, nrows=1).columns.tolist()

    def load_columns(self, paths):
        # paths: dict of paths
        for f in paths:
            self.tables.append(f)
            self.columns[f] = pd.read_csv(paths[f], nrows=1).columns.tolist()

    def get_cols_in_main_table(self):
        node = self.table_nodes['root'].children[0]
        return self.columns[node.name]

    def resample(self, tables=None, cols=None, problem=None, is_balanced=None, minority=None):
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

        # check whether the columns in main table exist
        # TODO
        t_cols = self.get_cols_in_main_table()

        # load main table with these columns
        if cols is None:
            df = self.load_main_tables(t_cols)
        else:
            pass

        # resampled in these tables

        # join the other columns in the other tables in it

        if problem == 'classification':
            if is_balanced:
                pass
            else:
                pass
             
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
