package sqldb

import (
	"regexp"
	"strings"
	"testing"
)

func TestSQLHolder(t *testing.T) {
	var s SQLHolder
	var runTimes uint32
	getSql := func() string {
		return s.SQL(nil, func(b *SQLBuilder) string {
			runTimes++
			return "s"
		})
	}

	if runTimes != 0 {
		t.Fatal("unexpected result")
	}
	if getSql() != "s" || runTimes != 1 {
		t.Fatal("unexpected result")
	}
	if getSql() != "s" || runTimes != 1 {
		t.Fatal("unexpected result")
	}
}

func TestSQLBuilder(t *testing.T) {
	p := NewTableParser()
	su := NewSQLUtil(p, Postgres{})
	sb := NewSQLBuilder(su)

	type Model struct {
		Id       string `sql:"pk"`
		Name     string
		Email    string
		Password string
	}
	var m Model

	type testCase struct {
		Expect string

		SQL string
	}
	cases := []testCase{
		{SQL: sb.Query(m, []string{"id"}, nil), Expect: "SELECT id FROM model"},
		{SQL: sb.Query(m, []string{"id"}, []string{"name"}), Expect: "SELECT id FROM model WHERE name = :name"},
		{SQL: sb.Query(m, []string{"id"}, []string{"name"}), Expect: "SELECT id FROM model WHERE name = :name"},
		{SQL: sb.Query(m, []string{"id"}, []string{"name", "email"}), Expect: "SELECT id FROM model WHERE name = :name AND email = :email"},
		{SQL: sb.Query(m, []string{}, []string{"name", "email"}), Expect: "SELECT id, password FROM model WHERE name = :name AND email = :email"},

		{SQL: sb.Delete(m, nil), Expect: "DELETE FROM model"},
		{SQL: sb.Delete(m, []string{"id"}), Expect: "DELETE FROM model WHERE id = :id"},
		{SQL: sb.Delete(m, []string{"name", "email"}), Expect: "DELETE FROM model WHERE name = :name AND email = :email"},
		{SQL: sb.DeleteByConds(m, ""), Expect: "DELETE FROM model"},
		{SQL: sb.DeleteByConds(m, "id != :id"), Expect: "DELETE FROM model WHERE id != :id"},

		{SQL: sb.Insert(m), Expect: "INSERT INTO model(id, name, email, password) VALUES(:id, :name, :email, :password)"},
		{SQL: sb.InsertUnique(m, []string{"id"}), Expect: "INSERT INTO model(id, name, email, password) SELECT :id, :name, :email, :password WHERE NOT EXISTS(SELECT 1 FROM model WHERE id = :id)"},
		{SQL: sb.InsertUnique(m, []string{"name", "email"}), Expect: "INSERT INTO model(id, name, email, password) SELECT :id, :name, :email, :password WHERE NOT EXISTS(SELECT 1 FROM model WHERE name = :name AND email = :email)"},

		{SQL: sb.Update(m, nil, nil), Expect: "UPDATE model SET id = :id, name = :name, email = :email, password = :password"},
		{SQL: sb.Update(m, nil, []string{"id"}), Expect: "UPDATE model SET name = :name, email = :email, password = :password WHERE id = :id"},
		{SQL: sb.Update(m, []string{"password"}, nil), Expect: "UPDATE model SET password = :password"},
		{SQL: sb.Update(m, []string{"password"}, []string{"id"}), Expect: "UPDATE model SET password = :password WHERE id = :id"},

		{SQL: sb.IsExist(m, "exist", nil), Expect: "SELECT EXISTS(SELECT 1 FROM model) AS exist"},
		{SQL: sb.IsExist(m, "exist", []string{"id"}), Expect: "SELECT EXISTS(SELECT 1 FROM model WHERE id = :id) AS exist"},

		{SQL: sb.MultiIsExist(CheckIsExistGroup{m, "exist", nil}), Expect: "SELECT EXISTS(SELECT 1 FROM model) AS exist"},
		{SQL: sb.MultiIsExist(CheckIsExistGroup{m, "exist", []string{"id"}}), Expect: "SELECT EXISTS(SELECT 1 FROM model WHERE id = :id) AS exist"},
		{SQL: sb.MultiIsExist(CheckIsExistGroup{m, "name", []string{"name"}}, CheckIsExistGroup{m, "email", []string{"email"}}), Expect: "SELECT EXISTS(SELECT 1 FROM model WHERE name = :name) AS name, EXISTS(SELECT 1 FROM model WHERE email = :email) AS email"},
	}
	r := regexp.MustCompile(" +")
	clean := func(s string) string {
		return r.ReplaceAllString(strings.TrimSpace(s), " ")
	}
	for i, c := range cases {
		got := clean(c.SQL)
		if c.Expect != got {
			t.Fatalf("%d: expect %s, nut got %s", i, c.Expect, got)
		}
	}
}
