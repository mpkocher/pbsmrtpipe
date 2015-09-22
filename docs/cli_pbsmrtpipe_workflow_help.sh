usage: pbsmrtpipe pipeline [-h] [--debug] -e ENTRY_POINTS [ENTRY_POINTS ...]
                           [-o OUTPUT_DIR] [--preset-xml PRESET_XML]
                           [--preset-rc-xml PRESET_RC_XML]
                           [--service-uri SERVICE_URI]
                           [--force-distributed | --local-only]
                           [--force-chunk-mode | --disable-chunk-mode]
                           pipeline_template_xml

positional arguments:
  pipeline_template_xml
                        Path to pipeline template XML file.

optional arguments:
  -h, --help            show this help message and exit
  --debug               Send logging info to stdout.
  -e ENTRY_POINTS [ENTRY_POINTS ...], --entry ENTRY_POINTS [ENTRY_POINTS ...]
                        Entry Points using 'entry_idX:/path/to/file.txt'
                        format.
  -o OUTPUT_DIR, --output-dir OUTPUT_DIR
                        Path to job output directory. Directory will be
                        created if it does not exist.
  --preset-xml PRESET_XML
                        Preset/Option XML file.
  --preset-rc-xml PRESET_RC_XML
                        Skipping loading preset from ENV var
                        'PB_SMRTPIPE_XML_PRESET' and Explicitly load the
                        supplied preset.xml
  --service-uri SERVICE_URI
                        Remote Webservices to send update and log status to.
                        (JSON file with host, port).
  --force-distributed   Override XML settings to enable distributed mode (if
                        cluster manager is provided)
  --local-only          Override XML settings to disable distributed mode. All
                        Task will be submitted to Michaels-MacBook-Pro.local
  --force-chunk-mode    Override to enable Chunk mode
  --disable-chunk-mode  Override to disable Chunk mode
