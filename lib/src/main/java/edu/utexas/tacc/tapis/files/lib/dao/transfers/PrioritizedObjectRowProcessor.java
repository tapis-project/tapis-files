package edu.utexas.tacc.tapis.files.lib.dao.transfers;

import edu.utexas.tacc.tapis.files.lib.models.PrioritizedObject;
import org.apache.commons.dbutils.BasicRowProcessor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

class PrioritizedObjectRowProcessor<T> extends BasicRowProcessor {
    private final BasicRowProcessor rowProcessor;
    Class<T> clazz;

    PrioritizedObjectRowProcessor(BasicRowProcessor rowProcessor, Class<T> clazz) {
        this.rowProcessor = rowProcessor;
        this.clazz = clazz;
    }

    @Override
    public PrioritizedObject toBean(ResultSet resultSet, Class type) throws SQLException {
        T object = rowProcessor.<T>toBean(resultSet, clazz);
        int priority = resultSet.getInt("row_number");
        return new PrioritizedObject(priority, object);
    }

    @Override
    public List<PrioritizedObject<T>> toBeanList(ResultSet resultSet, Class type) throws SQLException {
        List<PrioritizedObject<T>> list = new ArrayList<>();
        while (resultSet.next()) {
            list.add(toBean(resultSet, type));
        }
        return list;
    }
}
