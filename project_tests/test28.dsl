--
-- Control test for test21.dsl
--
-- Query in SQL:
-- SELECT col3 FROM tbl4_ctrl WHERE col2 >= 800 and col2 < 810;
--
load("../project_tests/data4_ctrl.csv")

s2=select(db1.tbl4ctrl.col2,800,810)
f2=fetch(db1.tbl4ctrl.col3,s2)
print(f2)
