LD_LIBRARY_PATH=./target/debug/ valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all  --error-limit=no  ./target/debug/examples/ffi

