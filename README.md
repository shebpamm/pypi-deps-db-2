## A dependency database for python packages on pypi

The data is updated twice each day via a github cron action. You can fork this repo and it should continue to update itself.

This data allows deterministic dependency resolution which is required by [mach-nix](https://github.com/DavHau/mach-nix) to generate reproducible python environments.

The data contains dependencies for sdist and wheel packages, though the sdist dependencies are not fully complete, since there is no fixed standard for declaring dependencies in sdist packages.
Most sdist packages which use setuptools/setup.py are contained.

---
### Maintaining your own fork
#### On Github
It will still have the cron action and you probably don't need to do anything.

#### Elsewhere
Use the included nix flake app to keep the data updated:
   - install nix: https://nixos.org/download.html
   - get a flakes capable shell
      ```shell
         nix-shell -p nixFlakes
      ```
   - execute
      ```shell
         nix run .#job-sdist-wheel
      ```
