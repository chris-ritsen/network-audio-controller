This is a python program for controlling Dante network audio devices (and possibly others in the future). It's very early, but the first goal is to do everything that Dante Controller can do.

[gearspace discussion](https://gearspace.com/board/music-computers/1221989-dante-routing-without-dante-controller-possible.html)

### Current features:

- Curses interface to display all device channels, devices names, IP addresses
- Logs Tx/Rx channels to a file
- Logs active subscriptions to a file
- Simple TUI menu (j/k up/down and tab for navigation)
- mDNS device discovery

### Planned features:

- AES67 device support
- Adding/removing subscriptions
- CLI interface
- Changing channel names
- Changing/displaying device settings (name, latency, encoding, sample rate, level controls, AES67 mode)
- Command prompt
- Signal presence indicator
