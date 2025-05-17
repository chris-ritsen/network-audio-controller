### Description

This is a Python program for controlling Dante network audio devices (and
possibly others in the future). It's early, so expect things to break or
switches to change. Use this at your own risk; it's not ready for anything
other than a test environment and could make the devices behave unexpectedly.

The first goal is to do everything that Dante Controller can do that would be
useful for control of the devices from a command-line interface or within
scripts.

For more information, check out the [gearspace discussion](https://gearspace.com/board/music-computers/1221989-dante-routing-without-dante-controller-possible.html).

### Demo

<p align="center"><img src="https://github.com/chris-ritsen/network-audio-controller/blob/master/demo/demo.gif?raw=true" alt="netctl usage demo" title="netctl usage demo"/></p>

### Features

* AVIO input/output gain control
* Add/remove subscriptions
* CLI
* Display active subscriptions, Rx and Tx channels, device names and
  addresses, subscription status
* JSON output
* Set device latency, sample rate, encoding
* Set/reset channel names, device names
* mDNS device discovery


### Installation

Use [uv](https://github.com/astral-sh/uv) to install and run:

```bash
uv venv
uv pip install -e .
```

To run:

```bash
uv run netaudio
```

### Development

After cloning the repo:

```bash
uv venv
uv pip install -e ".[dev]"
```

To run the CLI:

```bash
uv run netaudio
```

To run tests:

```bash
uv run pytest
```

### Documentation

* [Examples](https://github.com/chris-ritsen/network-audio-controller/wiki/Examples)
* [Technical details](https://github.com/chris-ritsen/network-audio-controller/wiki/Technical-details)
* [Testing](https://github.com/chris-ritsen/network-audio-controller/wiki/Testing)
