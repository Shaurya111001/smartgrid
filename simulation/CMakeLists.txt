
cmake_minimum_required(VERSION 3.16)
project(simulation LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(SIM_SOURCES
  main.cpp
  partition/partition_strategy.cpp
  domain/district.cpp
  io/result_writer.cpp
  benchmark/benchmark.cpp
  kafka/event_publisher.cpp
)

# Add all domain sources (nodes, producers, consumers, accumulators, grid)
file(GLOB DOMAIN_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/domain/*.cpp")
list(APPEND SIM_SOURCES ${DOMAIN_SRCS})

# Add scenario generators
file(GLOB SCENARIO_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/scenario/*.cpp")
list(APPEND SIM_SOURCES ${SCENARIO_SRCS})

# Optional MPI: prefer system MPI if available, otherwise build a single-process stub
find_package(MPI)
if(MPI_FOUND)
  message(STATUS "MPI found")
  add_definitions(-DHAVE_MPI_H)
  list(APPEND SIM_SOURCES mpi/mpi_manager.cpp)
else()
  message(STATUS "MPI not found; building with single-process stub")
  list(APPEND SIM_SOURCES mpi/mpi_manager_stub.cpp)
endif()

add_executable(simulation_app ${SIM_SOURCES})

if(MPI_FOUND)
  # Attach MPI include directories and libraries to the target so compiler can find mpi.h
  if(TARGET MPI::MPI_CXX)
    target_link_libraries(simulation_app PRIVATE MPI::MPI_CXX)
  else()
    if(DEFINED MPI_CXX_INCLUDE_DIRS)
      target_include_directories(simulation_app PRIVATE ${MPI_CXX_INCLUDE_DIRS})
    elseif(DEFINED MPI_INCLUDE_PATH)
      target_include_directories(simulation_app PRIVATE ${MPI_INCLUDE_PATH})
    endif()

    if(DEFINED MPI_CXX_LIBRARIES)
      target_link_libraries(simulation_app PRIVATE ${MPI_CXX_LIBRARIES})
    elseif(DEFINED MPI_LIBRARIES)
      target_link_libraries(simulation_app PRIVATE ${MPI_LIBRARIES})
    endif()
  endif()

  add_definitions(-DHAVE_MPI_H)
endif()

target_include_directories(simulation_app PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}
)

option(BUILD_WITH_KAFKA "Enable building the Kafka EventPublisher (requires librdkafka)" ON)

# Note: we avoid fetching heavy third-party projects during CMake configure to keep
# the build simple. The simulation code builds minimal JSON strings locally so no
# third-party JSON dependency is required.

if(BUILD_WITH_KAFKA)
  # Homebrew installs (esp. Apple Silicon /opt/homebrew) aren't always on CMake's
  # default search path, so hint explicitly in addition to the default search.
  find_library(RDKAFKA_LIB rdkafka HINTS /opt/homebrew/lib /usr/local/lib)
  # Homebrew installs rdkafka.h under include/librdkafka/, but the code includes
  # it as <rdkafka.h>, so the search must land on that subdirectory directly.
  find_path(RDKAFKA_INCLUDE_DIR rdkafka.h
    HINTS /opt/homebrew/include /usr/local/include
    PATH_SUFFIXES librdkafka)
  if(RDKAFKA_LIB AND RDKAFKA_INCLUDE_DIR)
    message(STATUS "Found librdkafka: ${RDKAFKA_LIB} (headers: ${RDKAFKA_INCLUDE_DIR})")
    target_include_directories(simulation_app PRIVATE ${RDKAFKA_INCLUDE_DIR})
    target_link_libraries(simulation_app PRIVATE ${RDKAFKA_LIB})
    target_compile_definitions(simulation_app PRIVATE RDKAFKA_AVAILABLE)
  else()
    message(WARNING "librdkafka not found. BUILD_WITH_KAFKA=ON but librdkafka missing. Try installing librdkafka (librdkafka-dev) or set BUILD_WITH_KAFKA=OFF.")
  endif()
endif()


