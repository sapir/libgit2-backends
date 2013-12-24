PROJECT(LIBGIT2-pgsql C)
CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

INCLUDE(../CMake/FindLibgit2.cmake)
FIND_PACKAGE(PostgreSQL)

# Build options
OPTION (BUILD_SHARED_LIBS "Build Shared Library (OFF for Static)" ON)
OPTION (BUILD_TESTS "Build Tests" ON)

# Build Release by default
IF (NOT CMAKE_BUILD_TYPE)
    SET(CMAKE_BUILD_TYPE "Release" CACHE STRING "Choose the type of build, options are: Debug Release RelWithDebInfo MinSizeRel." FORCE)
ENDIF ()

# Compile and link LIBGIT2
INCLUDE_DIRECTORIES(${LIBGIT2_INCLUDE_DIRS} ${PostgreSQL_INCLUDE_DIRS})
ADD_LIBRARY(git2-pgsql pgsql-odb.c pgsql-refdb.c)
TARGET_LINK_LIBRARIES(git2-pgsql ${LIBGIT2_LIBRARIES} ${PostgreSQL_LIBRARIES})
