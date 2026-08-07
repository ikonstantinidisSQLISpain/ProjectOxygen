"""Per-table row logic: `whoz_ingestion.shaping.<table>`.

One module per silver table, named for the entity that owns it. Nothing here imports
`pyspark.pipelines`, which is what makes every module in this folder importable from a plain
pytest session — see the package docstring in ../__init__.py.

    profile.py                         shape_profile() + PROFILE_COLUMNS / _HISTORY_COLUMNS
    profile_aptitudes.py               aptitudes_sql()
    profile_positions.py               positions_sql()
    profile_position_aptitude_refs.py  aptitude_refs_sql()
    profile_skill_ratings.py           skill_ratings_sql()
    profile_completion_rules.py        completion_rules_sql()
    talent.py                          shape_talent() + TALENT_COLUMNS / _HISTORY_COLUMNS
    talent_workspace_history.py        workspace_history_sql()

The entity prefix is the whole naming rule, and it earns its keep: these were one
`children.py` until recently, which put five profile collections and one *talent* collection
(`workspace_history_sql`) in the same file under a name that implied they were siblings. They
are not — they explode out of different bronze tables. A filename that names the owner makes
that impossible to misread, and it is why there is no `children/` folder: "child table" is a
relationship, not a category worth grouping by.

WHY THE EXPLODED QUERIES ARE SQL TEXT, once, rather than in six module headers.

They use `spark.sql` rather than the DataFrame API because `variant_explode` is a table-valued
generator, so it belongs in the FROM clause via LATERAL — there is no DataFrame equivalent that
unnests a VARIANT array in one step.

They live here rather than inline in `transformations/silver/` because a module importing
`pyspark.pipelines` cannot be imported outside a running Lakeflow pipeline. When these were
f-strings inside the `@dp.table` functions, the ten quality checks on these datasets — half of
every check in the project — could only be handed to `F.expr(sql)`, which parses a predicate but
**resolves no column names**: a check naming `proficiency_level` where the column is
`proficiency` passed the entire suite. Returning the SQL from a plain function lets a test
register a bronze fixture as a view, run the real query, and resolve the real checks against
real columns. See tests/layer3_rules/test_child_rules.py. Same seam as shape_profile(), same
reason.

Each takes the source relation as an **argument** rather than templating a module constant, so
the pipeline passes `STREAM(<bronze table>)` and a test passes a temp view name, and both run
the same text. A function taking `source` rather than a constant with a `{source}` placeholder,
because `str.format` would break on the first `{` a future query needs, and an f-string keeps
the style of the rest of the project.

Deliberately not empty: a zero-byte file is not deployed by `databricks bundle deploy`, which
would make this a namespace-package gap in the workspace copy only. Same trap as ../__init__.py
documents at length.
"""
