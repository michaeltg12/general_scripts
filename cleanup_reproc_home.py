from os import environ, path, walk, makedirs
from shutil import copyfile, rmtree
from distutils.dir_util import copy_tree
from sys import argv

msg = 'First arguement should be a dqr #.'
try:
    user_input = argv[1]
except IndexError:
    print(msg)
if user_input in ['-h', '--help']:
    print(msg)
else:
    dqr = user_input

archive_file_params = ['ncr_', 'conf', 'json', '.py', '.ipynb', 'log', '.sh', '.csh', '.bash', '.Ingest']
archive_folder_params = ['ncr_', 'script']
reproc_home = environ['REPROC_HOME']
post_proc = environ['POST_PROC']

clean_dir = path.join(reproc_home, dqr)
archive_dir = path.join(post_proc, dqr, 'auto_archive')
makedirs(archive_dir, exist_ok=True)

for dirName, subdirList, fileList in walk(clean_dir):
    print('Found directory: {}'.format(dirName))
    if any(param in dirName for param in archive_folder_params):
        src = dirName
        dest = path.join(archive_dir, dirName)
        print("Archiving directory: {}".format(src))
        copy_tree(src, dest)
    for fname in fileList:
        if any(param in fname for param in archive_file_params):
            src = path.join(dirName, fname)
            dest = path.join(archive_dir, fname)
            print("Archiving file: {}".format(fname))
            #print('{}'.format(src))
            #print('---> {}'.format(dest))
            try:
                copyfile(src, dest)
            except FileNotFoundError:
                print('\tFile DNE: {}'.format(fname))
print('REMOVING: {}'.format(clean_dir))
rmtree(clean_dir)
print('Finished cleaning: {}'.format(clean_dir))
