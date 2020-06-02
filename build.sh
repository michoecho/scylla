#!/bin/bash
set -e

# Install gzip, snappy and thrift >= 0.11.0 first

SCYLLA="$PWD"
P4S="$PWD/parquet4seastar"
P4S_INCLUDE="$P4S/include"
P4S_LIB="$P4S/build/libparquet4seastar.a"

if [ ! -d parquet4seastar ]; then
	git clone git@github.com:michoecho/parquet4seastar "$P4S"
fi

git submodule update --init
./configure.py --mode dev
ninja -C build/dev/seastar libseastar.a

cd "$P4S"
git checkout dev
mkdir -p build
cd build
cmake "-DCMAKE_PREFIX_PATH=$SCYLLA/build/dev/seastar" "-DCMAKE_MODULE_PATH=$SCYLLA/seastar/cmake" ..
make parquet4seastar apps -j4

cd "$SCYLLA"
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO
# EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES
# OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.
VIMARGS=(-N -u NONE -i NONE -n -c "set nomore")
vim $VIMARGS "+norm /rule cxxj0/-Ii-I$P4S_INCLUDE :x" build.ninja
vim $VIMARGS "+norm /seastar_libsf=lli$P4S_LIB :x" build.ninja

ninja build/dev/scylla
