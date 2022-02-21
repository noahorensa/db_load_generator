# module name
MODULE = db_load_generator

################################################################################

BUILD_DIR = build/$(shell uname -s)-$(shell uname -m)
LIB_DIR = lib/$(shell uname -s)-$(shell uname -m)

INCLUDES = -Iinclude -Ilibspl/include $(shell mysql_config --include)

LIBS = -Llibspl/lib/$(shell uname -s)-$(shell uname -m) -lspl $(shell mysql_config --libs)
LIB_DEPEND = libspl/lib/$(shell uname -s)-$(shell uname -m)/libspl.so

CXX = g++
CPPFLAGS = -Werror -Wall -Winline -Wpedantic
CXXFLAGS = -std=c++17 -march=native -pthread

AR = ar
ARFLAGS = rc

LDFLAGS = -Wl,-E -Wl,-export-dynamic
DEPFLAGS = -MM

SOURCES = $(wildcard src/*.cpp)
OBJ_FILES = $(SOURCES:src/%.cpp=$(BUILD_DIR)/%.o)

.PHONY : all libspl clean clean-dep

all : dblg

dblg : bin/dblg
	@echo "LN        $(MODULE)/$@"
	@ln -sf bin/dblg dblg

libspl :
	@$(MAKE) -C libspl --no-print-directory nodep="$(nodep)"

ifndef nodep
include $(SOURCES:src/%.cpp=.dep/%.d)
else
ifneq ($(nodep), true)
include $(SOURCES:src/%.cpp=.dep/%.d)
endif
endif

# cleanup

clean :
	@rm -rf build lib bin dblg
	@echo "Cleaned $(MODULE)/build/"
	@echo "Cleaned $(MODULE)/lib/"
	@echo "Cleaned $(MODULE)/bin/"
	@echo "Cleaned $(MODULE)/dblg"
	@$(MAKE) -C libspl --no-print-directory clean nodep="$(nodep)"

clean-dep :
	@rm -rf .dep
	@echo "Cleaned $(MODULE)/.dep/"
	@$(MAKE) -C libspl --no-print-directory clean-dep nodep="$(nodep)"

# dirs

.dep bin $(BUILD_DIR) $(LIB_DIR) :
	@echo "MKDIR     $(MODULE)/$@/"
	@mkdir -p $@

# core

bin/dblg : $(OBJ_FILES) $(LIB_DEPEND) | bin
	@echo "LD        $(MODULE)/$@"
	@$(CXX) $(CXXFLAGS) $(EXTRACXXFLAGS) $(OBJ_FILES) $(LD_FLAGS) $(LIBS) -o $@

$(LIB_DEPEND) : libspl

.dep/%.d : src/%.cpp | .dep
	@echo "DEP       $(MODULE)/$@"
	@set -e; rm -f $@; \
	$(CXX) $(DEPFLAGS) $(INCLUDES) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,$(BUILD_DIR)/\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

$(BUILD_DIR)/%.o : src/%.cpp | $(BUILD_DIR)
	@echo "CXX       $(MODULE)/$@"
	@$(CXX) -c $(CPPFLAGS) $(CXXFLAGS) $(EXTRACXXFLAGS) $(INCLUDES) $< -o $@
