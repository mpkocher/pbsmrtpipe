usage: pbsmrtpipe task [-h] [--debug] [--preset-rc-xml PRESET_RC_XML]
                       [--preset-xml PRESET_XML] [-o OUTPUT_DIR] -e
                       ENTRY_POINTS [ENTRY_POINTS ...]
                       task_id

positional arguments:
  task_id               Show details of registered Task by id.

optional arguments:
  -h, --help            show this help message and exit
  --debug               Print debug output to stdout.
  --preset-rc-xml PRESET_RC_XML
                        Skipping loading preset from ENV var
                        'PB_SMRTPIPE_XML_PRESET' and Explicitly load the
                        supplied preset.xml
  --preset-xml PRESET_XML
                        Preset/Option XML file.
  -o OUTPUT_DIR, --output-dir OUTPUT_DIR
                        Path to job output directory. Directory will be
                        created if it does not exist.
  -e ENTRY_POINTS [ENTRY_POINTS ...], --entry ENTRY_POINTS [ENTRY_POINTS ...]
                        Entry Points using 'entry_idX:/path/to/file.txt'
                        format.
