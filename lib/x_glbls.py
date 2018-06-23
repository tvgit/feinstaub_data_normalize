
# Sharing information via this module across >feinstaub_data_normalize_02.py< and its modules
# http://stackoverflow.com/questions/13034496/using-global-variables-between-files-in-python
#
import argparse

def make_arg_ns(origin = 'unknown !?'):
    global arg_ns

    arg_ns = argparse.Namespace()
    arg_ns.__origin__ =  str(origin)
    # optional args(ConfArgParser):
    arg_ns.fn_data_in = None
    arg_ns.fn_data_out = None
    arg_ns.dir = None

    # args for module: >fstb_dta_to_db_mod.py<
    arg_ns.db_JSON_dir = None
    arg_ns.db_JSON_fn = None
    arg_ns.files_JSON_dir = None



    # positional args(ConfArgParser):
    return arg_ns


def print_arg_ns():
    global arg_ns
    for key, value in sorted (vars(arg_ns).iteritems()):
        #print "key / value =  " + key, ' * ' , value
        print "arg_ns." + str(key) + ' = ' + str(value)


if __name__ == "__main__":
    arg_ns = make_arg_ns(r'x_glbls.py')
    print_arg_ns()
else:
    arg_ns = make_arg_ns(r'x_glbls.py')
    pass
# 2017_07_27-23_58_16