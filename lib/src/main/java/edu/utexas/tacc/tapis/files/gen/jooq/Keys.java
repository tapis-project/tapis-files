/*
 * This file is generated by jOOQ.
 */
package edu.utexas.tacc.tapis.files.gen.jooq;


import edu.utexas.tacc.tapis.files.gen.jooq.tables.FilesPostits;
import edu.utexas.tacc.tapis.files.gen.jooq.tables.records.FilesPostitsRecord;

import org.jooq.TableField;
import org.jooq.UniqueKey;
import org.jooq.impl.DSL;
import org.jooq.impl.Internal;


/**
 * A class modelling foreign key relationships and constraints of tables in
 * files.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class Keys {

    // -------------------------------------------------------------------------
    // UNIQUE and PRIMARY KEY definitions
    // -------------------------------------------------------------------------

    public static final UniqueKey<FilesPostitsRecord> FILES_POSTITS_PKEY = Internal.createUniqueKey(FilesPostits.FILES_POSTITS, DSL.name("files_postits_pkey"), new TableField[] { FilesPostits.FILES_POSTITS.ID }, true);
}
