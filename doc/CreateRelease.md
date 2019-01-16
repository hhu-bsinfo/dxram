# Create releases of DXRAM and all related repositories

Document for development of DXRAM.

Repositories (create releases in order due to depedencies):
* dxbuild
* dxutils
* dxmon
* dxnet
* dxmem
* dxlog
* dxram
* dxapps
* ibdxnet
* cdepl

For each repository:
* Rebase master to current development
* Create tag with version of current snapshot
* Push master and tag to shared repository
* Wait for travis to build and release the binaries (dependencies of further repositories)
* Switch to development branch
* Bump own version number + versions of deps in build.gradle
