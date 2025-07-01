# BACpypes

BACpypes provides a BACnet application layer and network layer written in Python for daemons, scripting, and graphical interfaces. This is the current project, not the one over on SourceForge.

[![Join the chat at https://gitter.im/JoelBender/bacpypes](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/JoelBender/bacpypes?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

[![Documentation Status](https://readthedocs.org/projects/bacpypes/badge/?version=latest)](http://bacpypes.readthedocs.io/en/latest/?badge=latest)

## Installation

```bash
pip install bacpypes
```

To use the latest code from GitHub:

```bash
git clone https://github.com/JoelBender/bacpypes.git
cd bacpypes
python setup.py install
```

## Usage

Sample applications demonstrating common BACnet tasks can be found in the [`samples/`](samples) directory. For example, to see available options for the WhoIs/IAm example:

```bash
python samples/WhoIsIAm.py --help
```

## Documentation

Extensive tutorials and guides are available in the [`doc/`](doc) directory or online at [Read the Docs](https://bacpypes.readthedocs.io/). The tutorials walk through getting started and explain each sample in detail.
