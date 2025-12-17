# Priority Conflict Resolution Guide

## 🔴 CRITICAL (Grammar & Error Codes) - 4 files
1. asterixdb/asterix-lang-sqlpp/src/main/javacc/SQLPP.jj
2. asterixdb/asterix-common/src/main/java/org/apache/asterix/common/exceptions/ErrorCode.java
3. asterixdb/asterix-common/src/main/resources/asx_errormsg/en.properties
4. hyracks-fullstack/hyracks/hyracks-api/src/main/java/org/apache/hyracks/api/exceptions/ErrorCode.java
5. hyracks-fullstack/hyracks/hyracks-api/src/main/resources/errormsg/en.properties

## 🟠 HIGH (Core Syntax & Metadata) - 8 files
6. asterixdb/asterix-lang-common/src/main/java/org/apache/asterix/lang/common/base/Statement.java
7. asterixdb/asterix-metadata/src/main/java/org/apache/asterix/metadata/MetadataManager.java
8. asterixdb/asterix-metadata/src/main/java/org/apache/asterix/metadata/MetadataNode.java
9. asterixdb/asterix-metadata/src/main/java/org/apache/asterix/metadata/api/IMetadataManager.java
10. asterixdb/asterix-metadata/src/main/java/org/apache/asterix/metadata/api/IMetadataNode.java
11. asterixdb/asterix-metadata/src/main/java/org/apache/asterix/metadata/bootstrap/MetadataBootstrap.java
12. asterixdb/asterix-metadata/src/main/java/org/apache/asterix/metadata/bootstrap/MetadataRecordTypes.java
13. asterixdb/asterix-common/src/main/java/org/apache/asterix/common/metadata/MetadataConstants.java

## 🟡 MEDIUM (Query Translator & Execution) - 8 files
14. asterixdb/asterix-app/src/main/java/org/apache/asterix/app/translator/QueryTranslator.java
15. asterixdb/asterix-algebra/src/main/java/org/apache/asterix/translator/IStatementExecutor.java
16. asterixdb/asterix-app/src/main/java/org/apache/asterix/hyracks/bootstrap/CCApplication.java
17. asterixdb/asterix-algebra/src/main/java/org/apache/asterix/compiler/provider/SqlppCompilationProvider.java
18. asterixdb/asterix-algebra/src/main/java/org/apache/asterix/translator/ResultMetadata.java
19. asterixdb/asterix-metadata/src/main/java/org/apache/asterix/metadata/declared/MetadataProvider.java
20. asterixdb/asterix-metadata/src/main/java/org/apache/asterix/metadata/MetadataTransactionContext.java
21. hyracks-fullstack/hyracks/hyracks-api/src/main/java/org/apache/hyracks/api/application/ICCApplication.java

## 🟢 LOW (HTTP Servlets & Result Handling) - 7 files
22. asterixdb/asterix-app/src/main/java/org/apache/asterix/api/http/server/NCQueryServiceServlet.java
23. asterixdb/asterix-app/src/main/java/org/apache/asterix/api/http/server/QueryResultApiServlet.java
24. asterixdb/asterix-app/src/main/java/org/apache/asterix/api/http/server/QueryServiceServlet.java
25. asterixdb/asterix-app/src/main/java/org/apache/asterix/app/result/JobResultCallback.java
26. asterixdb/asterix-app/src/main/java/org/apache/asterix/app/result/ResponseMetrics.java
27. asterixdb/asterix-app/src/main/java/org/apache/asterix/app/result/fields/MetricsPrinter.java
28. hyracks-fullstack/hyracks/hyracks-api/src/main/java/org/apache/hyracks/api/job/JobFlag.java

## 🟢 LOW (Visitors & Config) - 10 files
29. asterixdb/asterix-lang-common/src/main/java/org/apache/asterix/lang/common/visitor/FormatPrintVisitor.java
30. asterixdb/asterix-lang-common/src/main/java/org/apache/asterix/lang/common/visitor/base/AbstractQueryExpressionVisitor.java
31. asterixdb/asterix-lang-common/src/main/java/org/apache/asterix/lang/common/visitor/base/ILangVisitor.java
32. asterixdb/asterix-common/src/main/java/org/apache/asterix/common/config/CompilerProperties.java
33. asterixdb/asterix-common/src/main/java/org/apache/asterix/common/config/OptimizationConfUtil.java
34. asterixdb/asterix-common/src/main/java/org/apache/asterix/common/metadata/IMetadataLockUtil.java
35. asterixdb/asterix-metadata/src/main/java/org/apache/asterix/metadata/utils/MetadataLockUtil.java
36. hyracks-fullstack/algebricks/algebricks-core/src/main/java/org/apache/hyracks/algebricks/core/config/AlgebricksConfig.java
37. hyracks-fullstack/algebricks/algebricks-core/src/main/java/org/apache/hyracks/algebricks/core/rewriter/base/PhysicalOptimizationConfig.java
38. hyracks-fullstack/hyracks/hyracks-examples/hyracks-integration-tests/src/test/java/org/apache/hyracks/tests/integration/AbstractMultiNCIntegrationTest.java

## ⚪ OPTIONAL (Test & Misc) - 10 files  
39. asterixdb/asterix-app/src/test/java/org/apache/asterix/api/common/AsterixHyracksIntegrationUtil.java
40. asterixdb/asterix-app/src/test/resources/log4j2-asterixdb-test.xml
41. asterixdb/asterix-runtime/src/test/java/org/apache/asterix/runtime/job/resource/JobCapacityControllerTest.java
42. asterixdb/NOTICE
43. hyracks-fullstack/NOTICE
44-47. hyracks dataflow files (4 files - consider moving to scheduler branch)
