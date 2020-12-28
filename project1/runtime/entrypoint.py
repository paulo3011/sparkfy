#!/usr/bin/python

"""etl docker entrypoint."""
# https://stackabuse.com/executing-shell-commands-with-python/
# https://www.tutorialspoint.com/python/python_command_line_arguments.htm

import subprocess

# import os
import sys
import getopt

# from typing import Dict
# from typing_extensions import Literal
from etl import main as run_etl

ACTIONS = ["runjob", "inspect", "runjob_and_inspect"]


def _runcmd(cmd):
    print(cmd)
    params = cmd.split(" ")
    cmd_result = subprocess.run(params)
    return cmd_result


# python runtime/entrypoint.py -a inspect
def _inspect():
    print("inspecting image")
    cmd = f"tail -f /dev/null"
    _runcmd(cmd)


def _runjob():
    print("starting ETL")
    run_etl(host="postgres")


def _runjob_and_inspect():
    _runjob()
    _inspect()


def _run(action: str):
    print("running action => ", action)

    if action == "runjob":
        _runjob()
    elif action == "inspect":
        _inspect()
    elif action == "runjob_and_inspect":
        _runjob_and_inspect()


def usage():
    """
    Prints how to use this.
    """
    print("usage: python entrypoint.py -a <action>")
    print("usage: python entrypoint.py --action <action>")
    print("Actions:", ACTIONS)


def _main(argv):
    action: str = ""
    try:
        action = ACTIONS[0]
        # shortopts is the string of option letters that the script wants to recognize, with options that require an argument followed by acolon
        # opts that dont need args dont need to be separate by acolon eg: hi: -> h (help without args) and i -> input file name with arg (name of file)
        opts, args = getopt.getopt(argv, "ha:", ["help", "action="])
        # print("main args >>>", args, "opts", opts)
    except getopt.GetoptError as ex:
        print("Wrong parameters!", ex)
        usage()
        sys.exit(2)

    for opt, arg in opts:
        # print("arg >> ", arg, "opt >>", opt)
        if opt in ("-h", "--help"):
            usage()
            sys.exit()
        elif opt in ("-a", "--action"):
            if arg in ACTIONS:
                action = arg
            else:
                print("Invalid --action. Valid values are: ", ACTIONS)
                sys.exit(2)

    _run(action)


if __name__ == "__main__":
    _main(sys.argv[1:])
