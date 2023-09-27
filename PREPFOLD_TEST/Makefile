#  Makefile for PRESTO
#   by Scott M. Ransom

# OS type
OS = Linux

UNAME = $(shell uname)
ifeq ($(UNAME),Darwin)
OS = OSX
endif
OUTPUT_DIR = /hercules/scratch/vishnu/SLURM_PULSARMINER/PREPFOLD_TEST

# Linux is the first choice
ifeq ($(OS),Linux)
	LIBSUFFIX = .so
	LIBCMD = -shared
# else assume Darwin (i.e. OSX)
else
	LIBSUFFIX = .dylib
	LIBCMD = -dynamiclib
endif

# How to link with some needed libraries of PGPLOT
X11LINK := $(shell pkg-config --libs x11)
PNGLINK := $(shell pkg-config --libs libpng)

# Include and link information for PGPLOT v5.X (including shared libs!)
# Typically you need to have your PGPLOT_DIR environment variable set
PGPLOTINC = -I$(PGPLOT_DIR)
PGPLOTLINK = -L$(PGPLOT_DIR) -lcpgplot -lpgplot $(X11LINK) $(PNGLINK)

# Include and link information for the FFTW 3.X single-precision library
FFTINC := $(shell pkg-config --cflags fftw3f)
FFTLINK := $(shell pkg-config --libs fftw3f)

# Include and link information for the GLIB 2.0 library
GLIBINC := $(shell pkg-config --cflags glib-2.0)
GLIBLINK := $(shell pkg-config --libs glib-2.0)

# Include and link information for CFITSIO
CFITSIOINC := $(shell pkg-config --cflags cfitsio)
CFITSIOLINK := $(shell pkg-config --libs cfitsio)

# The standard PRESTO libraries to link into executables
PRESTOLINK = $(CFITSIOLINK) -L$(PRESTO)/lib -lpresto $(FFTLINK)

CC = gcc
#CC = clang-3.6
FC = gfortran

# Set this to true if you want to use OpenMP.  false otherwise
USEOPENMP = true

# Set this to true if you want to profile.
USEPROFILE = false

# Very recent Intel CPUs might see a few percent speedup using -mavx
CFLAGS = -I$(PRESTO)/include $(GLIBINC) $(CFITSIOINC) $(PGPLOTINC) $(FFTINC) \
	-DUSEFFTW -DUSEMMAP -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 \
	-g -Wall -W -fPIC -O3 -ffast-math
CLINKFLAGS = $(CFLAGS)

# Add some GCC-specific flags that are useful
ifeq ($(CC),gcc)
	CFLAGS += -Wno-unused-result -Wno-unused-but-set-variable \
		-Wno-unused-but-set-parameter
# The following is great as long as your GCC is > v 4.9
#	CFLAGS += -fdiagnostics-color=auto
endif

# NOTE:  Be careful of upping the optimization on the
#        FFLAGs.  Certain compilers (i.e. on Intel Macs) will
#        cause errors for the code in least_squares.f
FFLAGS = -g -fPIC
FLINKFLAGS = $(FFLAGS)

# Add flags we need for openmp
ifeq ($(USEOPENMP),true)
	ifeq ($(CC),icc)
		OMPFLAGS = -openmp
		CFLAGS += $(OMPFLAGS)
		FLINKFLAGS += $(OMPFLAGS)
# for GCC and clang
	else
		OMPFLAGS = -fopenmp
		CFLAGS += $(OMPFLAGS)
		FLINKFLAGS += $(OMPFLAGS)
	endif
endif

# Add flags we need for profiling (including making a static libpresto)
ifeq ($(USEPROFILE),true)
	CFLAGS += -pg
	FFLAGS += -pg
	LIBSUFFIX = .a
endif

ifeq ($(LIBSUFFIX),.so)
	LINKCOMMAND = $(CC) $(LIBCMD) $(OMPFLAGS) $(FFTLINK) -o
else
	LINKCOMMAND = ar rcs
endif

# Add to the search path for the executables
VPATH = ../lib:../bin

# When modifying the CLIG files, the is the location of the clig binary
CLIG = clig

# Rules for CLIG generated files
%_cmd.c : ../clig/%_cmd.cli
	cd ../clig ; $(CLIG) -o $*_cmd -d $<
	mv ../clig/$*_cmd.h ../include/
	mv ../clig/$*_cmd.c .
	cp ../clig/$*.1 ../docs/

# PRESTOOBJS = amoeba.o atwood.o barycenter.o birdzap.o cand_output.o\
# 	characteristics.o cldj.o chkio.o corr_prep.o corr_routines.o\
# 	correlations.o database.o dcdflib.o dispersion.o\
# 	fastffts.o fftcalls.o fminbr.o fold.o fresnl.o ioinf.o\
# 	get_candidates.o iomak.o ipmpar.o maximize_r.o maximize_rz.o\
# 	maximize_rzw.o median.o minifft.o misc_utils.o clipping.o\
# 	orbint.o output.o read_fft.o readpar.o responses.o\
# 	rzinterp.o rzwinterp.o select.o sorter.o swapendian.o\
# 	transpose.o twopass.o twopass_real_fwd.o\
# 	twopass_real_inv.o vectors.o multifiles.o mask.o\
# 	fitsfile.o hget.o hput.o imio.o djcl.o range_parse.o

INSTRUMENTOBJS = backend_common.o zerodm.o sigproc_fb.o psrfits.o

# Use old header reading stuff for readfile
READFILEOBJS = $(INSTRUMENTOBJS) multibeam.o bpp.o spigot.o \
	wapp.o wapp_head_parse.o wapp_y.tab.o

PLOT2DOBJS = powerplot.o xyline.o

BINARIES = prepfold

all: libpresto slalib binaries

# Default indentation is K&R style with no-tabs,
# an indentation level of 4 (default), and a line-length of 85
indent:
	indent -kr -nut -l85 *.c
	rm *.c~

prep:
	touch *_cmd.c

makewisdom:
	$(CC) $(CLINKFLAGS) -o $@ makewisdom.c $(FFTLINK)
	./makewisdom
	cp fftw_wisdom.txt $(PRESTO)/lib

timetest:
	$(CC) -o $@ timetest.c
	./timetest
	rm -f timetest

libpresto: libpresto$(LIBSUFFIX)

libpresto$(LIBSUFFIX): $(PRESTOOBJS)
	$(LINKCOMMAND) /hercules/scratch/vishnu/SLURM_PULSARMINER/PREPFOLD_TEST/LIB/$@ $(PRESTOOBJS)

slalib: libsla$(LIBSUFFIX)
	cd slalib ; $(FC) -o sla_test sla_test.f -fno-second-underscore -L$(PRESTO)/lib -lsla
	slalib/sla_test

libsla$(LIBSUFFIX):
	cd slalib ; $(FC) $(FFLAGS) -fno-second-underscore -c -I. *.f *.F
	rm slalib/sla_test.o
	cd slalib ; $(FC) $(LIBCMD) -o $(PRESTO)/lib/libsla$(LIBSUFFIX) -fno-second-underscore *.o

binaries: $(BINARIES)



# prepfold: prepfold_cmd.c prepfold_cmd.o prepfold.o prepfold_utils.o prepfold_plot.o least_squares.o polycos.o readpar.o $(INSTRUMENTOBJS) $(PLOT2DOBJS) libpresto
# 	$(FC) $(FLINKFLAGS) -o $(PRESTO)/bin/$@ prepfold.o prepfold_utils.o prepfold_plot.o prepfold_cmd.o least_squares.o polycos.o readpar.o $(PLOT2DOBJS) $(INSTRUMENTOBJS) $(LAPACKLINK) $(PRESTOLINK) $(PGPLOTLINK) -lcfitsio -lm

prepfold: prepfold_cmd.c prepfold_cmd.o prepfold.o prepfold_utils.o prepfold_plot.o least_squares.o polycos.o readpar.o $(INSTRUMENTOBJS) $(PLOT2DOBJS) libpresto
	$(FC) $(FLINKFLAGS) -o $(OUTPUT_DIR)/$@ prepfold.o prepfold_utils.o prepfold_plot.o prepfold_cmd.o least_squares.o polycos.o readpar.o $(PLOT2DOBJS) $(INSTRUMENTOBJS) $(LAPACKLINK) $(PRESTOLINK) $(PGPLOTLINK) -lcfitsio -lm


clean:
	rm -f *.o *~ *#
	rm -f slalib/*.o slalib/sla_test

cleaner: clean
	cd ../bin ; rm -f $(BINARIES)
	rm -f $(PRESTO)/lib/libpresto.* $(PRESTO)/lib/libsla.*

squeaky:  cleaner
	rm -f *.dat *.fft *.inf fftw_wisdom.txt
	rm -f core *.win* *.ps *_rzw *.tmp
	cd $(PRESTO)/clig ; rm -f *# *~
	cd $(PRESTO)/docs ; rm -f *# *~
	cd $(PRESTO)/python ; rm -f *# *~ *.o *.pyc *.pyo
	cd $(PRESTO)/include ; rm -f *# *~
