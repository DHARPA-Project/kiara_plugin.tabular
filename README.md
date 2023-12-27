[![PyPI status](https://img.shields.io/pypi/status/kiara_plugin.tabular.svg)](https://pypi.python.org/pypi/kiara_plugin.tabular/)
[![PyPI version](https://img.shields.io/pypi/v/kiara_plugin.tabular.svg)](https://pypi.python.org/pypi/kiara_plugin.tabular/)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/kiara_plugin.tabular.svg)](https://pypi.python.org/pypi/kiara_plugin.tabular/)
[![Build Status](https://img.shields.io/endpoint.svg?url=https%3A%2F%2Factions-badge.atrox.dev%2FDHARPA-Project%2Fkiara%2Fbadge%3Fref%3Ddevelop&style=flat)](https://actions-badge.atrox.dev/DHARPA-Project/kiara_plugin.tabular/goto?ref=develop)
[![Coverage Status](https://coveralls.io/repos/github/DHARPA-Project/kiara_plugin.tabular/badge.svg?branch=develop)](https://coveralls.io/github/DHARPA-Project/kiara_plugin.tabular?branch=develop)
[![Code style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

# [**kiara**](https://dharpa.org/kiara.documentation) plugin: (tabular)

kiara data-types and modules for working with tables and databases.

 - Documentation: [https://DHARPA-Project.github.io/kiara_plugin.tabular](https://DHARPA-Project.github.io/kiara_plugin.tabular)
 - Code: [https://github.com/DHARPA-Project/kiara_plugin.tabular](https://github.com/DHARPA-Project/kiara_plugin.tabular)
 - `kiara`: [https://dharpa.org/kiara.documentation](https://dharpa.org/kiara.documentation)

## Description

TODO

## Development

### Requirements

- Python (version >= 3.8)
- pip, virtualenv
- git
- make (on Linux / Mac OS X -- optional)


### Prepare development environment

If you only want to work on the modules, and not the core *Kiara* codebase, follow the instructions below. Otherwise, please
check the notes on how to setup a *Kiara* development environment under (TODO).

#### Using `pixi` (recommended)

The recommended way to setup a development environment is to use [pixi](https://github.com/prefix-dev/pixi). Check out [their install instructions](https://github.com/prefix-dev/pixi#installation).

Once you have `pixi` installed, you need to initialize the environment once:

```
pixi run install-dev-env
```

You also need to do this whenever a depdendency of this plugin is updated (for example the core `kiara` package).

Once that is done, you can enter the environment with:

```
pixi shell
```

This will start a sub-shell with the virtual environment activated, and all dependencies of the plugin package installed. To confirm it works, you can run any `kiara` command:

```
kiara --version
# or
kiara operation list
# or
...
...
```

Once you are finished with your development session, you can exit the sub-shell as you would normally do in such cases:

```
exit
```

Alternatively, you can also run the `kiara` executable directly, it is located in `.pixi/env/bin/kiara`. So either adapt your `PATH` variable, or do something like:

```
.pixi/env/bin/kiara operation list
```

In most cases it's recommended to use a pixi shell though.


### Using pre-defined development-related tasks

The included `pixi.toml` file includes some useful tasks that help with development:

- `pixi run pre-commit-check`: runs a set of checks against all files
- `pixi run tests`: runs the unit tests
- `pixi run mypy`: run mypy checks

## Copyright & license

This project is MPL v2.0 licensed, for the license text please check the [LICENSE](/LICENSE) file in this repository.
