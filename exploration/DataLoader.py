import os
import pandas as pd


class DataLoader:
    def __init__(self, root=None):
        self.root = root
        self.files = {}
        self.filenames = []
        pass

    def load_all_csv(self):
        dfs = {}
        for f in self.files:
            dfs[f] = pd.read_csv(self.files[f])
        return dfs

    def load_csv(self, fn):
        assert fn in self.filenames, "get file name first"
        return pd.read_csv(self.files[fn])

    def get_file_name(self, path=None, filters=None, ignore=None):

        # path: root to search
        # filters: which type of file will be remained
        # ignore: which file will be ignored
        if path is not None:
            self.root = os.path.realpath(path)
        else:
            assert self.root is not None, "root can't be None, reset \
                    root first"
        for root, dirs, files in os.walk(self.root):
            for f in files:
                fn, fe = os.path.splitext(f)
                if fe in filters and f not in ignore:

                    if dirs == []:
                        p = os.path.join(root, f)
                    else:
                        p = os.path.join(root, dirs)
                        p = os.path.join(p, f)
                    self.files[f] = p
                    self.filenames.append(f)

        return self.filenames

    def ret_file_names(self):
        assert len(self.filenames) != 0, "get file name first"
        return self.filenames
        
    def show_files(self):
        for f in self.files:
            print(self.files[f])


if __name__ == '__main__':
    dataloader = DataLoader()
    dataloader.get_file_name('./', '.csv', None)
    dataloader.show_files()

