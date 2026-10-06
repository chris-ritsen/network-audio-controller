# netaudio icons

`proposal/` holds the proposed icon set, a Dante routing matrix with three
green crosspoints:

| File | Use |
| --- | --- |
| `netaudio-mark.svg` | Primary mark, 48px and up. Lines and labels use `currentColor`. |
| `netaudio-16.svg` | Favicon, drawn on the 16px pixel grid. Follows light/dark mode. |
| `netaudio-32.svg` | 32px version, pixel-snapped, with checkmarks. |
| `netaudio-mark-mono.svg` | Single colour, checkmarks knocked out. |
| `netaudio-app-icon.svg` | Dark app tile for iOS and desktop. |

`candidates/` holds the earlier exploration.

## Candidates

Draft SVG icon candidates for netaudio. Each file in `candidates/` is a
64x64 monochrome icon drawn with `currentColor`, so it works on light and dark
backgrounds and inside a coloured app tile.

Regenerate the comparison page after editing a candidate:

```bash
python3 design/icons/build_gallery.py
```

Then open `design/icons/gallery.html` in a browser.
