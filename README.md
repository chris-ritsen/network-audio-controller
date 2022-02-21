
### Description

This is a python program for controlling Dante network audio devices (and possibly others in the future). It's very early, so expect things to break or switches to change.  Use this at your own risk; it's not ready for anything other than a test environment and could make the devices behave unexpectedly. The first goal is to do everything that Dante Controller can do that would be useful for control of the devices from a command-line interface or within scripts.

For more information, check out the [gearspace discussion](https://gearspace.com/board/music-computers/1221989-dante-routing-without-dante-controller-possible.html).

### Features

- AVIO input/output gain control
- Add/remove subscriptions
- CLI
- Display active subscriptions, Rx and Tx channels, devices names and addresses, subscription status
- JSON output
- Set device latency, sample rate, encoding
- Set/reset channel names, device names
- mDNS device discovery

### In progress

- Gather information from multicast traffic (make, model, lock status, subscription changes)

### Planned features

- AES67 device support
- Change channel/device names without affecting existing subscriptions
- Change/display device settings (AES67 mode)
- Client/server modes
- Command prompt
- Control of Shure wireless devices ([Axient receivers](https://pubs.shure.com/view/command-strings/AD4/en-US.pdf) and [PSM transmitters](https://pubs.shure.com/view/command-strings/PSM1000/en-US.pdf))
- Signal presence indicator
- Stand-alone command API
- TUI
- Web application UI
- XML output (such as a Dante preset file)

### Installation

To install from PyPI on most systems, use pip or pipx:

```bash
pipx install netaudio
```

```bash
pip install netaudio
```

To install the package from a clone:
```bash
pipx install --force --include-deps .
```

#### Arch Linux

To install from AUR, build the package with
[aur/python-netaudio](https://aur.archlinux.org/packages/python-netaudio).
For development, install the following packages:

```bash
pacman -S community/python-pipx community/python-poetry
```

#### MacOS

For development, install the following packages:

```bash
brew install pipx poetry
brew link pipx poetry
```

### Usage

To run without installing:
```bash
poetry install
poetry run netaudio
```

Then run `netaudio`

### Documentation

- [Examples](https://github.com/chris-ritsen/network-audio-controller/wiki/Examples)
- [Technical details](https://github.com/chris-ritsen/network-audio-controller/wiki/Technical-details)
- [Testing](https://github.com/chris-ritsen/network-audio-controller/wiki/Testing)
