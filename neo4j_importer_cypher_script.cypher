:param {
  // Define the file path root and the individual file names required for loading.
  // https://neo4j.com/docs/operations-manual/current/configuration/file-locations/
  file_path_root: 'file:///', // Change this to the folder your script can access the files at.
  file_0: 'author.csv',
  file_1: 'publication.csv',
  file_2: 'authored.csv'
};

// CONSTRAINT creation
// -------------------
//
// Create node uniqueness constraints, ensuring no duplicates for the given node label and ID property exist in the database. This also ensures no duplicates are introduced in future.
//
// NOTE: The following constraint creation syntax is valid for database version 4.4.0 and above.
CREATE CONSTRAINT `imp_uniq_Authors_Authors_full_name` IF NOT EXISTS
FOR (n: `Authors`)
REQUIRE (n.`Authors_full_name`) IS UNIQUE;
CREATE CONSTRAINT `imp_uniq_Publications_PMID` IF NOT EXISTS
FOR (n: `Publications`)
REQUIRE (n.`PMID`) IS UNIQUE;

// If your database version is older than 4.4.0, please use the constraint creation syntax below:
// CREATE CONSTRAINT `imp_uniq_Authors_Authors_full_name` IF NOT EXISTS
// ON (n: `Authors`)
// ASSERT n.`Authors_full_name` IS UNIQUE;
// CREATE CONSTRAINT `imp_uniq_Publications_PMID` IF NOT EXISTS
// ON (n: `Publications`)
// ASSERT n.`PMID` IS UNIQUE;

:param {
  idsToSkip: []
};

// NODE load
// ---------
//
// Load nodes in batches, one node label at a time. Nodes will be created using a MERGE statement to ensure a node with the same label and ID property remains unique. Pre-existing nodes found by a MERGE statement will have their other properties set to the latest values encountered in a load file.
//
// NOTE: Any nodes with IDs in the 'idsToSkip' list parameter will not be loaded.
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Authors_full_name` IN $idsToSkip AND NOT row.`Authors_full_name` IS NULL
CALL {
  WITH row
  MERGE (n: `Authors` { `Authors_full_name`: row.`Authors_full_name` })
  SET n.`Authors_full_name` = row.`Authors_full_name`
  SET n.`Author_Affiliate` = row.`Author_Affiliate`
  SET n.`Country` = row.`Country`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_1) AS row
WITH row
WHERE NOT row.`PMID` IN $idsToSkip AND NOT toInteger(trim(row.`PMID`)) IS NULL
CALL {
  WITH row
  MERGE (n: `Publications` { `PMID`: toInteger(trim(row.`PMID`)) })
  SET n.`PMID` = toInteger(trim(row.`PMID`))
  SET n.`Title` = row.`Title`
  SET n.`Journal` = row.`Journal`
  SET n.`Year` = toInteger(trim(row.`Year`))
  SET n.`Rank` = row.`Rank`
} IN TRANSACTIONS OF 10000 ROWS;


// RELATIONSHIP load
// -----------------
//
// Load relationships in batches, one relationship type at a time. Relationships are created using a MERGE statement, meaning only one relationship of a given type will ever be created between a pair of nodes.
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_2) AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Authors` { `Authors_full_name`: row.`Authors_full_name` })
  MATCH (target: `Publications` { `PMID`: toInteger(trim(row.`PMID`)) })
  MERGE (source)-[r: `Authored`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;
