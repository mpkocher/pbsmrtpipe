usage: pbsmrtpipe pipeline [-h] [--preset-rc-xml PRESET_RC_XML]
                           [--preset-xml PRESET_XML] -e ENTRY_POINTS
                           [ENTRY_POINTS ...] [-o OUTPUT_DIR] [--debug]
                           pipeline_template_xml

positional arguments:
  pipeline_template_xml
                        Path to pipeline template XML file.

optional arguments:
  -h, --help            show this help message and exit
  --preset-rc-xml PRESET_RC_XML
                        Skipping loading preset from ENV var
                        'PB_SMRTPIPE_XML_PRESET' and Explicitly load the
                        supplied preset.xml
  --preset-xml PRESET_XML
                        Preset/Option XML file.
  -e ENTRY_POINTS [ENTRY_POINTS ...], --entry ENTRY_POINTS [ENTRY_POINTS ...]
                        Entry Points using 'entry_idX:/path/to/file.txt'
                        format.
  -o OUTPUT_DIR, --output-dir OUTPUT_DIR
                        Path to job output directory. Directory will be
                        created if it does not exist.
  --debug               Print debug output to stdout.
