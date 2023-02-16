/*
 * This file is generated by jOOQ.
 */
package edu.utexas.tacc.tapis.files.gen.jooq.tables;


import edu.utexas.tacc.tapis.files.gen.jooq.Indexes;
import edu.utexas.tacc.tapis.files.gen.jooq.Keys;
import edu.utexas.tacc.tapis.files.gen.jooq.Public;
import edu.utexas.tacc.tapis.files.gen.jooq.tables.records.FilesPostitsRecord;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.jooq.Field;
import org.jooq.ForeignKey;
import org.jooq.Function12;
import org.jooq.Index;
import org.jooq.Name;
import org.jooq.Record;
import org.jooq.Records;
import org.jooq.Row12;
import org.jooq.Schema;
import org.jooq.SelectField;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.TableOptions;
import org.jooq.UniqueKey;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.TableImpl;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class FilesPostits extends TableImpl<FilesPostitsRecord> {

    private static final long serialVersionUID = 1L;

    /**
     * The reference instance of <code>public.files_postits</code>
     */
    public static final FilesPostits FILES_POSTITS = new FilesPostits();

    /**
     * The class holding records for this type
     */
    @Override
    public Class<FilesPostitsRecord> getRecordType() {
        return FilesPostitsRecord.class;
    }

    /**
     * The column <code>public.files_postits.id</code>.
     */
    public final TableField<FilesPostitsRecord, String> ID = createField(DSL.name("id"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>public.files_postits.systemid</code>.
     */
    public final TableField<FilesPostitsRecord, String> SYSTEMID = createField(DSL.name("systemid"), SQLDataType.CLOB, this, "");

    /**
     * The column <code>public.files_postits.path</code>.
     */
    public final TableField<FilesPostitsRecord, String> PATH = createField(DSL.name("path"), SQLDataType.CLOB, this, "");

    /**
     * The column <code>public.files_postits.alloweduses</code>.
     */
    public final TableField<FilesPostitsRecord, Integer> ALLOWEDUSES = createField(DSL.name("alloweduses"), SQLDataType.INTEGER, this, "");

    /**
     * The column <code>public.files_postits.timesused</code>.
     */
    public final TableField<FilesPostitsRecord, Integer> TIMESUSED = createField(DSL.name("timesused"), SQLDataType.INTEGER, this, "");

    /**
     * The column <code>public.files_postits.jwtuser</code>.
     */
    public final TableField<FilesPostitsRecord, String> JWTUSER = createField(DSL.name("jwtuser"), SQLDataType.CLOB, this, "");

    /**
     * The column <code>public.files_postits.jwttenantid</code>.
     */
    public final TableField<FilesPostitsRecord, String> JWTTENANTID = createField(DSL.name("jwttenantid"), SQLDataType.CLOB, this, "");

    /**
     * The column <code>public.files_postits.owner</code>.
     */
    public final TableField<FilesPostitsRecord, String> OWNER = createField(DSL.name("owner"), SQLDataType.CLOB, this, "");

    /**
     * The column <code>public.files_postits.tenantid</code>.
     */
    public final TableField<FilesPostitsRecord, String> TENANTID = createField(DSL.name("tenantid"), SQLDataType.CLOB, this, "");

    /**
     * The column <code>public.files_postits.expiration</code>.
     */
    public final TableField<FilesPostitsRecord, LocalDateTime> EXPIRATION = createField(DSL.name("expiration"), SQLDataType.LOCALDATETIME(6).nullable(false).defaultValue(DSL.field("timezone('utc'::text, now())", SQLDataType.LOCALDATETIME)), this, "");

    /**
     * The column <code>public.files_postits.created</code>.
     */
    public final TableField<FilesPostitsRecord, LocalDateTime> CREATED = createField(DSL.name("created"), SQLDataType.LOCALDATETIME(6).nullable(false).defaultValue(DSL.field("timezone('utc'::text, now())", SQLDataType.LOCALDATETIME)), this, "");

    /**
     * The column <code>public.files_postits.updated</code>.
     */
    public final TableField<FilesPostitsRecord, LocalDateTime> UPDATED = createField(DSL.name("updated"), SQLDataType.LOCALDATETIME(6).nullable(false).defaultValue(DSL.field("timezone('utc'::text, now())", SQLDataType.LOCALDATETIME)), this, "");

    private FilesPostits(Name alias, Table<FilesPostitsRecord> aliased) {
        this(alias, aliased, null);
    }

    private FilesPostits(Name alias, Table<FilesPostitsRecord> aliased, Field<?>[] parameters) {
        super(alias, null, aliased, parameters, DSL.comment(""), TableOptions.table());
    }

    /**
     * Create an aliased <code>public.files_postits</code> table reference
     */
    public FilesPostits(String alias) {
        this(DSL.name(alias), FILES_POSTITS);
    }

    /**
     * Create an aliased <code>public.files_postits</code> table reference
     */
    public FilesPostits(Name alias) {
        this(alias, FILES_POSTITS);
    }

    /**
     * Create a <code>public.files_postits</code> table reference
     */
    public FilesPostits() {
        this(DSL.name("files_postits"), null);
    }

    public <O extends Record> FilesPostits(Table<O> child, ForeignKey<O, FilesPostitsRecord> key) {
        super(child, key, FILES_POSTITS);
    }

    @Override
    public Schema getSchema() {
        return aliased() ? null : Public.PUBLIC;
    }

    @Override
    public List<Index> getIndexes() {
        return Arrays.asList(Indexes.FILES_POSTITS_EXPIRATION_INDEX);
    }

    @Override
    public UniqueKey<FilesPostitsRecord> getPrimaryKey() {
        return Keys.FILES_POSTITS_PKEY;
    }

    @Override
    public FilesPostits as(String alias) {
        return new FilesPostits(DSL.name(alias), this);
    }

    @Override
    public FilesPostits as(Name alias) {
        return new FilesPostits(alias, this);
    }

    @Override
    public FilesPostits as(Table<?> alias) {
        return new FilesPostits(alias.getQualifiedName(), this);
    }

    /**
     * Rename this table
     */
    @Override
    public FilesPostits rename(String name) {
        return new FilesPostits(DSL.name(name), null);
    }

    /**
     * Rename this table
     */
    @Override
    public FilesPostits rename(Name name) {
        return new FilesPostits(name, null);
    }

    /**
     * Rename this table
     */
    @Override
    public FilesPostits rename(Table<?> name) {
        return new FilesPostits(name.getQualifiedName(), null);
    }

    // -------------------------------------------------------------------------
    // Row12 type methods
    // -------------------------------------------------------------------------

    @Override
    public Row12<String, String, String, Integer, Integer, String, String, String, String, LocalDateTime, LocalDateTime, LocalDateTime> fieldsRow() {
        return (Row12) super.fieldsRow();
    }

    /**
     * Convenience mapping calling {@link SelectField#convertFrom(Function)}.
     */
    public <U> SelectField<U> mapping(Function12<? super String, ? super String, ? super String, ? super Integer, ? super Integer, ? super String, ? super String, ? super String, ? super String, ? super LocalDateTime, ? super LocalDateTime, ? super LocalDateTime, ? extends U> from) {
        return convertFrom(Records.mapping(from));
    }

    /**
     * Convenience mapping calling {@link SelectField#convertFrom(Class,
     * Function)}.
     */
    public <U> SelectField<U> mapping(Class<U> toType, Function12<? super String, ? super String, ? super String, ? super Integer, ? super Integer, ? super String, ? super String, ? super String, ? super String, ? super LocalDateTime, ? super LocalDateTime, ? super LocalDateTime, ? extends U> from) {
        return convertFrom(toType, Records.mapping(from));
    }
}
