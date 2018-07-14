import pandas as pd
import os
from six.moves import xrange


class Representation:
    def __init__(self, keys, target):
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
        pass

    def _check_data_type(self):
        # check the type of database
        # TODO
        pass

    def _check_keys(self, columns):
        cnt = 0
        for key in self.keys:
            if key in columns:
                cnt += 1

        if cnt == len(self.keys):
            return True
        else:
            return False

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
                print('keys: '+''.join(node.keys))
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
