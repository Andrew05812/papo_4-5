package com.example;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.cdc.CdcHandler;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import java.util.Optional;

public class UniversityHierarchyCdcHandler extends CdcHandler {

    public UniversityHierarchyCdcHandler(MongoSinkTopicConfig config) {
        super(config);
    }

    private static BsonString safeGetString(BsonDocument doc, String key) {
        BsonValue val = doc.get(key);
        if (val == null || val.isNull()) {
            return new BsonString("");
        }
        if (val.isString()) {
            return val.asString();
        }
        return new BsonString(val.toString());
    }

    private static String safeGetStr(BsonDocument doc, String key) {
        return safeGetString(doc, key).getValue();
    }

    @Override
    public Optional<WriteModel<BsonDocument>> handle(SinkDocument sinkDoc) {
        if (sinkDoc == null || !sinkDoc.getValueDoc().isPresent()) {
            return Optional.empty();
        }
        BsonDocument valueDoc = sinkDoc.getValueDoc().get();
        BsonString op = valueDoc.getString("op");
        if (op == null) {
            return Optional.empty();
        }

        BsonValue afterVal = valueDoc.get("after");
        BsonDocument after = (afterVal != null && afterVal.isDocument()) ? afterVal.asDocument() : null;

        BsonValue beforeVal = valueDoc.get("before");
        BsonDocument before = (beforeVal != null && beforeVal.isDocument()) ? beforeVal.asDocument() : null;

        String opStr = op.getValue();
        switch (opStr) {
            case "c":
            case "r":
                return (after != null) ? Optional.ofNullable(buildUpsert(after)) : Optional.empty();
            case "u":
                return (after != null) ? Optional.ofNullable(buildUpsert(after)) : Optional.empty();
            case "d":
                return (before != null) ? Optional.ofNullable(buildDelete(before)) : Optional.empty();
            default:
                return Optional.empty();
        }
    }

    private String detectTable(BsonDocument doc) {
        if (doc.containsKey("founded_year") && doc.containsKey("address") && !doc.containsKey("university_id")) {
            return "university";
        }
        if (doc.containsKey("university_id") && doc.containsKey("dean")) {
            return "institute";
        }
        if (doc.containsKey("institute_id") && doc.containsKey("head") && !doc.containsKey("speciality_id")) {
            return "department";
        }
        if (doc.containsKey("degree_level") && doc.containsKey("duration_years") && !doc.containsKey("department_id")) {
            return "speciality";
        }
        if (doc.containsKey("department_id") && doc.containsKey("speciality_id") && doc.containsKey("is_primary")) {
            return "department_specialities";
        }
        return "unknown";
    }

    private UpdateOneModel<BsonDocument> buildUpsert(BsonDocument doc) {
        String table = detectTable(doc);
        switch (table) {
            case "university":
                return buildUniversityUpsert(doc);
            case "institute":
                return buildInstituteUpsert(doc);
            case "department":
                return buildDepartmentUpsert(doc);
            case "speciality":
                return buildSpecialityUpsert(doc);
            case "department_specialities":
                return buildDeptSpecUpsert(doc);
            default:
                return null;
        }
    }

    private UpdateOneModel<BsonDocument> buildUniversityUpsert(BsonDocument doc) {
        BsonString idVal = safeGetString(doc, "id");
        Bson foundedYear = (doc.get("founded_year") != null && !doc.get("founded_year").isNull())
                ? doc.get("founded_year")
                : new BsonString("");
        return new UpdateOneModel<>(
                Filters.eq("_id", idVal),
                Updates.combine(
                        Updates.set("name", safeGetString(doc, "name")),
                        Updates.set("short_name", safeGetString(doc, "short_name")),
                        Updates.set("address", safeGetString(doc, "address")),
                        Updates.set("founded_year", foundedYear),
                        Updates.setOnInsert("institutes", new BsonDocument())
                ),
                new UpdateOptions().upsert(true)
        );
    }

    private UpdateOneModel<BsonDocument> buildInstituteUpsert(BsonDocument doc) {
        String univId = safeGetStr(doc, "university_id");
        String instId = safeGetStr(doc, "id");
        if (univId.isEmpty() || instId.isEmpty()) {
            return null;
        }
        return new UpdateOneModel<>(
                Filters.eq("_id", new BsonString(univId)),
                Updates.combine(
                        Updates.set("institutes." + instId + ".id", safeGetString(doc, "id")),
                        Updates.set("institutes." + instId + ".name", safeGetString(doc, "name")),
                        Updates.set("institutes." + instId + ".short_name", safeGetString(doc, "short_name")),
                        Updates.set("institutes." + instId + ".dean", safeGetString(doc, "dean")),
                        Updates.setOnInsert("institutes." + instId + ".departments", new BsonDocument())
                ),
                new UpdateOptions().upsert(false)
        );
    }

    private UpdateOneModel<BsonDocument> buildDepartmentUpsert(BsonDocument doc) {
        String instId = safeGetStr(doc, "institute_id");
        String deptId = safeGetStr(doc, "id");
        if (instId.isEmpty() || deptId.isEmpty()) {
            return null;
        }
        return new UpdateOneModel<>(
                Filters.eq("institutes." + instId, new BsonString(instId)),
                Updates.combine(
                        Updates.set("institutes." + instId + ".departments." + deptId + ".id", safeGetString(doc, "id")),
                        Updates.set("institutes." + instId + ".departments." + deptId + ".institute_id", safeGetString(doc, "institute_id")),
                        Updates.set("institutes." + instId + ".departments." + deptId + ".name", safeGetString(doc, "name")),
                        Updates.set("institutes." + instId + ".departments." + deptId + ".short_name", safeGetString(doc, "short_name")),
                        Updates.set("institutes." + instId + ".departments." + deptId + ".head", safeGetString(doc, "head")),
                        Updates.setOnInsert("institutes." + instId + ".departments." + deptId + ".specialities", new BsonDocument())
                ),
                new UpdateOptions().upsert(false)
        );
    }

    private UpdateOneModel<BsonDocument> buildSpecialityUpsert(BsonDocument doc) {
        String specId = safeGetStr(doc, "id");
        if (specId.isEmpty()) {
            return null;
        }
        Bson durationYears = (doc.get("duration_years") != null && !doc.get("duration_years").isNull())
                ? doc.get("duration_years")
                : new BsonString("");
        return new UpdateOneModel<>(
                Filters.exists("_id"),
                Updates.combine(
                        Updates.set("specialities_pool." + specId + ".id", safeGetString(doc, "id")),
                        Updates.set("specialities_pool." + specId + ".name", safeGetString(doc, "name")),
                        Updates.set("specialities_pool." + specId + ".code", safeGetString(doc, "code")),
                        Updates.set("specialities_pool." + specId + ".degree_level", safeGetString(doc, "degree_level")),
                        Updates.set("specialities_pool." + specId + ".duration_years", durationYears)
                ),
                new UpdateOptions().upsert(false)
        );
    }

    private UpdateOneModel<BsonDocument> buildDeptSpecUpsert(BsonDocument doc) {
        String deptId = safeGetStr(doc, "department_id");
        String specId = safeGetStr(doc, "speciality_id");
        if (deptId.isEmpty() || specId.isEmpty()) {
            return null;
        }
        BsonValue isPrimary = (doc.get("is_primary") != null && !doc.get("is_primary").isNull())
                ? doc.get("is_primary")
                : new BsonBoolean(true);
        return new UpdateOneModel<>(
                Filters.exists("_id"),
                Updates.set("dept_spec_links." + deptId + "." + specId, isPrimary),
                new UpdateOptions().upsert(false)
        );
    }

    private WriteModel<BsonDocument> buildDelete(BsonDocument doc) {
        String table = detectTable(doc);
        switch (table) {
            case "university":
                return new DeleteOneModel<>(Filters.eq("_id", safeGetString(doc, "id")));
            case "institute": {
                String instId = safeGetStr(doc, "id");
                if (instId.isEmpty()) return null;
                return new UpdateOneModel<>(Filters.exists("_id"), Updates.unset("institutes." + instId));
            }
            case "department": {
                String deptId = safeGetStr(doc, "id");
                String instId = safeGetStr(doc, "institute_id");
                if (deptId.isEmpty() || instId.isEmpty()) return null;
                return new UpdateOneModel<>(Filters.exists("_id"), Updates.unset("institutes." + instId + ".departments." + deptId));
            }
            case "speciality": {
                String specId = safeGetStr(doc, "id");
                if (specId.isEmpty()) return null;
                return new UpdateOneModel<>(Filters.exists("_id"), Updates.unset("specialities_pool." + specId));
            }
            case "department_specialities": {
                String deptId = safeGetStr(doc, "department_id");
                String specId = safeGetStr(doc, "speciality_id");
                if (deptId.isEmpty() || specId.isEmpty()) return null;
                return new UpdateOneModel<>(Filters.exists("_id"), Updates.unset("dept_spec_links." + deptId + "." + specId));
            }
            default:
                return null;
        }
    }
}
