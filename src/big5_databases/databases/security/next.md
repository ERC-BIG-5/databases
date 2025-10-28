# next.md

## jsonpaths:

- [x] 1. ok, jsonpath_extractor still seems to do some manual replacement: _replace_at_path
write yourself a small test file, where you try out replacing value/removing them, just with the "update" of jsonpath-ng
after your experimentation, clean up all the functions in there. I dont want any custom path resolving. just robust jsonpath-ng calls! 
SEE THE DEMO src/big5_databases/databases/security/demo_jsonpathng_update.py


- [] 3. fix those issues:
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

you can use root() which is imported by tools. or just mandate to run scripts with the project root as working path

- [x] 4. remove this from all generated files:
#!/usr/bin/env python3
-*- coding: utf-8 -*-
