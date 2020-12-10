# Installing pipestat

Install pipestat from [GitHub releases](https://github.com/pepkit/pipestat/releases) or from PyPI with `pip`:

- `pip install --user pipestat`: install into user space.
- `pip install --user --upgrade pipestat`: update in user space.
- `pip install pipestat`: install into an active virtual environment.
- `pip install --upgrade pipestat`: update in virtual environment.

See if your install worked by calling `pipestat -h` on the command line. If the `pipestat` executable is not in your `$PATH`, append this to your `.bashrc` or `.profile` (or `.bash_profile` on macOS):
```console
export PATH=~/.local/bin:$PATH
```