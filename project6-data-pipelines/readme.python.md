# Tricks for python enviormment

1. After Python 3.3+ is no longer necessary use __init__.py to load modules. The old way with __init__.py files still works as in Python 2.

    ```shell
    $ mkdir -p /tmp/test_init
    $ touch /tmp/test_init/module.py /tmp/test_init/__init__.py
    $ tree -at /tmp/test_init
    /tmp/test_init
    ├── module.py
    └── __init__.py
    $ python3

    >>> import sys
    >>> sys.path.insert(0, '/tmp')
    >>> from test_init import module
    >>> import test_init.module

    $ rm -f /tmp/test_init/__init__.py
    $ tree -at /tmp/test_init
    /tmp/test_init
    └── module.py
    $ python3

    >>> import sys
    >>> sys.path.insert(0, '/tmp')
    >>> from test_init import module
    >>> import test_init.module

    $ # seealso https://stackoverflow.com/questions/448271/what-is-init-py-for#:~:text=The%20__init__.py%20file%20makes%20Python%20treat%20directories,the%20submodules%20to%20be%20exported.
    ```

    seealso:
    - https://stackoverflow.com/questions/37139786/is-init-py-not-required-for-packages-in-python-3-3
    - https://www.python.org/dev/peps/pep-0420/

2. Unresolved import warnings on VSCODE

    2.1 If you're getting a warning about an unresolved import, first ensure that the package is installed into your environment if it is a library (pip, pipenv, etc). If the warning is about importing your own code (and not a library), continue reading.

    2.2 The language server treats the workspace root (i.e. folder you have opened) as the main root of user module imports. This means that if your imports are not relative to this path, the language server will not be able to find them. This is common for users who have a src directory which contains their code, a directory for an installable package, etc. __These extra roots must be specified to the language server__. The easiest way to do this (with the VS Code Python extension) is to create a workspace configuration which sets __python.autoComplete.extraPaths__. For example, if a project uses a src directory, then create a file .vscode/settings.json in the workspace with the contents:

    ```json
    {
        "python.autoComplete.extraPaths": ["./src"]
    }
    ```

    This list can be extended to other paths within the workspace (or even with code outside the workspace in more complicated setups). Relative paths will be taken as relative to the workspace root.

    This list may also be configured using the PYTHONPATH environment variable, either set directly, or via a .env file in the workspace root (if using the Python extension):

    ```env
    PYTHONPATH=./src
    ```

    seealso: https://github.com/microsoft/python-language-server/blob/master/TROUBLESHOOTING.md#unresolved-import-warnings