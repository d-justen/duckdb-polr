add_library_unity(duckdb_operator_polr OBJECT physical_adaptive_union.cpp
                  physical_multiplexer.cpp routing_strategy.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_operator_polr>
    PARENT_SCOPE)
