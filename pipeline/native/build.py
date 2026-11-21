"""Build the local CPython parser; compiler required only at build time."""
from pathlib import Path
import shlex
import subprocess
import sysconfig

source = Path(__file__).resolve().parent
output = source.parent / ("_fastparse" + sysconfig.get_config_var("EXT_SUFFIX"))
command = shlex.split(sysconfig.get_config_var("LDSHARED"))
command += ["-O3", "-std=c11", "-D_POSIX_C_SOURCE=200809L", "-fPIC", "-pthread",
            "-I" + sysconfig.get_path("include"), str(source / "parser.c"), "-o", str(output)]
subprocess.run(command, check=True)
print(output)
