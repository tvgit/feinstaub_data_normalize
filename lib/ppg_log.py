#! /usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "rh"
__date__   = "$05.05.2015 21:55:22$"

import os
import sys
import inspect
import logging
import logging.handlers

p_logger = None  #  global


def scriptinfo():
    '''
    Returns a dictionary with information about the running top level Python
    script:
    ---------------------------------------------------------------------------
    dir:    directory containing script or compiled executable
    name:   name of script or executable
    source: name of source code file
    ---------------------------------------------------------------------------
    "name" and "source" are identical if and only if running interpreted code.
    When running code compiled by py2exe or cx_freeze, "source" contains
    the name of the originating Python script.
    If compiled by PyInstaller, "source" contains no meaningful information.
    http://code.activestate.com/recipes/579018-python-determine-name-and-directory-of-the-top-lev/
    '''

    import os, sys, inspect
    #---------------------------------------------------------------------------
    # scan through call stack for caller information
    #---------------------------------------------------------------------------
    for part in inspect.stack():
        # skip system calls
        if part[1].startswith("<"):
            continue
        if part[1].upper().startswith(sys.exec_prefix.upper()):
            continue
        trc = part[1]

    # trc contains highest level calling script name
    # check if we have been compiled
    if getattr(sys, 'frozen', False):
        scriptdir, scriptname = os.path.split(sys.executable)
        return {"dir": scriptdir,
                "name": scriptname,
                "source": trc}

    # from p_here on, we are in the interpreted case
    scriptdir, trc = os.path.split(trc)
    # if trc did not contain directory information,
    # the current working directory is what we need
    if not scriptdir:
        scriptdir = os.getcwd()

    scr_dict ={"name": trc,
               "source": trc,
               "dir": scriptdir}
    return scr_dict


def here(txt='', level=2):
    """ print txt; echoes from the stack the callee """
    mssge = inspect.stack()[level][3] + ': '
    if mssge.find('module') > -1 :
        prog_info = scriptinfo()
        mssge = prog_info['name']
        # print "prog_info['dir']", prog_info['dir']
        # print "prog_info['name']", prog_info['name']
        # print "prog_info['source']", prog_info['source']
    if txt: print txt, mssge
    return mssge


def p_log_dir_make(log_dir):
    """ checks, eventually creates sub_dir """
    mssge_00 = 'checking log_dir:  >' + log_dir + '<'
    log_dir = os.path.normpath(log_dir)
    if (os.path.isabs(log_dir)):
        print ( mssge_00  + '< is absolute, should be relative (i.e. ./log).')
        sys.exit() # exit !
    if (log_dir == ''):
        log_dir = '.'
    if not os.path.exists(log_dir):
        print mssge_00 + 'creating dir: >' + log_dir + '<',
        try:
            os.makedirs(log_dir)
            print '  ok'
        except IOError:
            print '  failed!'
    else:
        #print ' -> dir exists.'
        pass
    return log_dir

def p_log_fn_check(log_fn):
    """ checks extension >.log< """
    log_bn, log_ext = os.path.splitext(log_fn)
    if (log_ext != '.log'):
        log_ext = '.log'
    log_fn = log_bn + log_ext
    return log_fn


def p_log_init(log_dir='log', log_fn='default'):
    """ init log: checks/creates log-dir (subdir); log-file (*.log); log-format """
    global p_logger  # global !!
    # dir & basename of fn_log
    log_dir   = p_log_dir_make(log_dir)
    log_fn    = p_log_fn_check(log_fn)              # check if log_fn == *.log
    log_pn_fn = os.path.join(log_dir, log_fn)       # == ./log/*.log
    # Set basic logging format ... and logging level
    format = '%(levelname)-10s %(asctime)-12s  %(message)s'
    logging.basicConfig(level=logging.INFO, format=format)
    # create logger
    p_logger = logging.getLogger('pyprogen_Logger')
    # no logging to console:  http://stackoverflow.com/questions/2266646
    p_logger.propagate = False
    # Create rotating file handler
    file_handler = logging.handlers.RotatingFileHandler(log_pn_fn, maxBytes=0, backupCount=1, )
    # I wonder why I have to define the format once again (for the logfile)?
    fmt = logging.Formatter(format, datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(fmt)
    file_handler.setLevel(logging.INFO)
    # Add the log message handler to the logger
    p_logger.addHandler(file_handler)
    print 'Logger started: log file: >' + '.' + os.path.sep + log_pn_fn
    # prnt_log_pn_fn = os.path.join(os.path.dirname(__file__), log_pn_fn)
    # print 'Logger started: log file: >', prnt_log_pn_fn
    p_logger.setLevel('INFO')
    line = '-' * 40 # + ' ' + str(p_logger.level)
    p_logger.info(line)


def p_log_start(txt=''):
    """ start the global logger """
    global p_logger
    if p_logger:
        act_Level = p_logger.getEffectiveLevel()
        p_logger.setLevel(logging.INFO)
        p_log_this(here('', 2) + txt + ' Logging started')
        p_logger.setLevel(act_Level)

def p_log_end(txt=''):
    """ p_exit the global logger """
    global p_logger
    if p_logger:
        p_log_this(here('', 2) + txt + ' Logging end')
        logging.shutdown()
        print 'Logger ended'


def p_log_this(txt='', level=''):
    # type: (object, object) -> object
    # type: (object, object) -> object
    """ log_s mssge with the global logger """
    global p_logger
    # testen ob string oder integer. Wenn string -> Integer umwandeln
    if p_logger:
        if type(level) == str:
            level = level.lower()
            if level in ['debug','info','warning', 'error', 'critical']:
                lvl = {'debug':50,'info':40,'warning':30, 'error':20, 'critical' : 10}.get(level)
            elif level <> '':
                lvl = 20
                p_logger.log(lvl, here('', 2) + 'unknown logging level: >' + level + '<')
            else:
                lvl = p_logger.level
        else:
            lvl = p_logger.level
        p_logger.log(lvl, here('', 2) + txt)
    else:
        print 'No logging!' + here('', 2) + txt


# def p_log_this_org(txt='', level=''):
#     """ log_s mssge with the global logger """
#     global p_logger
#     # testen ob string oder integer. Wenn string -> Integer umwandeln
#     if p_logger:
#         if type(level) == str:
#             level = level.lower()
#             if level in ['debug', 'info', 'warning', 'error', 'critical']:
#                 lvl = {'debug': 50, 'info': 40, 'warning': 30, 'error': 20, 'critical': 10}.get(level)
#             elif level <> '':
#                 lvl = 20
#             else:
#                 lvl = p_logger.level
#         else:
#             lvl = p_logger.level
#         p_logger.log(lvl, here('', 2) + txt)
#     else:
#         print 'No logging!' + here('', 2) + txt
#

def p_log_this(txt='', level='', callee=True):
    # type: (object, object, object) -> object
    # type: (object, object, object) -> object
    """ log_s mssge with the global logger """
    global p_logger
    # testen ob string oder integer. Wenn string -> Integer umwandeln
    if p_logger:
        lvl = p_logger.level
        if type(level) == str:
            level = level.lower()
            if level in ['debug', 'info', 'warning', 'error', 'critical']:
                lvl = {'debug': 50, 'info': 40, 'warning': 30, 'error': 20, 'critical': 10}.get(level)
            elif level <> '':
                lvl = 20
                msge = 'unknown logging level: >' + level + '<'
                # p_logger.log(lvl, here('', 2) + 'unknown logging level: >' + level + '<')

        msge = txt
        # p_logger.log(lvl, here('', 2) + txt)

        if callee:
            msge = here('', 2) + msge
            # p_logger.log(lvl, here('', 2) + 'unknown logging level: >' + level + '<')
        p_logger.log(lvl, msge)
    else:
        print 'No logging!' + here('', 2) + txt


def p_log_status(txt=''):
    global p_logger
    if p_logger:
        print '>> logger status:', txt
        print '   logger = . . . . . ', p_logger
        print '   logger.level = . . ', p_logger.level
        print '   logger.eff level = ', p_logger.getEffectiveLevel()


if __name__ == "__main__":
    print ('ppg_log.py')
    p_log_init(log_dir='', log_fn='ppg_log')
    p_log_start('ppg_log')
    p_log_this ('test 01 ')
    p_log_this ('level = debug', 'debug')
    p_log_this ('level = info', 'info')
    p_log_this ('level = warning', 'warning')
    p_log_this ('level = error', 'error')
    p_log_this ('level = critical', 'critical')
    p_log_end  ('ppg_log')

    sys.exit('Program gentle exiting.')
else:
    # logger = p_log_init(log_fn='default')
    pass
