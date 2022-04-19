# Arrow Flight-SQL example

## Setup

In order to run this example you need to set up a new environment on your machine. 
Follow these steps to do so (thanks to David Li!).

1. Make sure you have [miniconda](https://docs.conda.io/en/latest/miniconda.html) installed on your machine. 
2. Install Mamba: `conda install -c conda-forge mamba`.
3. Now, create a new dev environment
```bash
$ mamba create -n flight-sql -c conda-forge python=3.9
```
4. Clone Arrow repository: `https://github.com/apache/arrow.git`.
5. Install dependencies and Arrow inside the environment.
```bash
$ conda activate flight-sql
$ mamba install -c conda-forge compilers
$ mamba install -c conda-forge --file ci/conda_env_cpp.txt
$ mkdir -p build && cd build
$ cmake ../cpp -GNinja -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=$CONDA_PREFIX -DARROW_FLIGHT=ON -DARROW_FLIGHT_SQL=ON 
$ ninja install # Will install into your Conda prefix
```
6. Build the example.
```bash
$ mkdir build && cd build
$ cmake .. -GNinja -DCMAKE_PREFIX_PATH=$CONDA_PREFIX/lib/cmake/arrow
$ ninja && ./flight_sql
```