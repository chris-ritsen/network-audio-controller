This is a python program for controlling Dante network audio devices (and possibly others in the future). It's very early, but the first goal is to do everything that Dante Controller can do.

[gearspace discussion](https://gearspace.com/board/music-computers/1221989-dante-routing-without-dante-controller-possible.html)

### Current features:

- CLI
- Display active subscriptions, Rx, Tx, devices
- mDNS device discovery
- JSON output

### Planned features:

- AES67 device support
- Adding/removing subscriptions
- Changing channel names
- Changing/displaying device settings (name, latency, encoding, sample rate, level controls, AES67 mode)
- Command prompt
- Control of Shure wireless devices ([Axient receivers](https://pubs.shure.com/view/command-strings/AD4/en-US.pdf) and [PSM transmitters](https://pubs.shure.com/view/command-strings/PSM1000/en-US.pdf))
- Signal presence indicator
- TUI

### Dependences

Install these dependencies with pip:

- argcomplete
- zeroconf

Then run with `./dante_controller.py`
