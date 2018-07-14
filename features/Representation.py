import pandas as pd


class Representation:
    def __init__(self, keys, target):
        """
        keys: key columns, with it one can identify the row, i.e. uid
        target: columns which to predict
        """
        self.keys = keys
        self.target = target
        self.tables = []
        self.columns = {}
        self.table_nodes = {}
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
            t2 = node.name
            keys = self._find_common_keys(t1, t2)
            if len(keys) > 0:
                return True, keys, t2

        return False, [], None

    def create_table_tree(self):
        # create root
        self.table_nodes['root'] = table_node('root', [])
        cnt = 0
        t_tables = self.tables
        while cnt != len(self.tables):
            li = []
            for t in t_tables:
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
                    continue

            for t in li:
                t_tables.remove(t)

    def load_columns(self, paths):
        # paths: dict of paths
        for f in paths:
            self.tables.append(f)
            self.columns[f] = pd.read_csv(paths[f], nrows=1).columns.tolist()


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

        assert len(keys) != 0, "this table node has a wrong parent"
        return keys

