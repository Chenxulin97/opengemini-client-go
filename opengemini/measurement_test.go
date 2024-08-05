package opengemini

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestClientShowTagKeys(t *testing.T) {
	c := testDefaultClient(t)
	databaseName := randomDatabaseName()
	err := c.CreateDatabase(databaseName)
	require.Nil(t, err)
	measurement := randomMeasurement()
	cmd := fmt.Sprintf("CREATE MEASUREMENT %s (tag1 TAG,tag2 TAG,tag3 TAG, field1 INT64 FIELD, field2 BOOL, field3 STRING, field4 FLOAT64)", measurement)
	_, err = c.Query(Query{Command: cmd, Database: databaseName})
	assert.Nil(t, err)
	showKeyCmd := fmt.Sprintf("SHOW TAG KEYS FROM %s limit 3 OFFSET 0", measurement)
	tagKeyResult, err := c.ShowTagKeys(databaseName, showKeyCmd)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(tagKeyResult))
	err = c.DropDatabase(databaseName)
	require.Nil(t, err)
}

func TestClient_ShowFieldKeys(t *testing.T) {
	c := testDefaultClient(t)
	databaseName := randomDatabaseName()
	err := c.CreateDatabase(databaseName)
	require.Nil(t, err)
	measurement := randomMeasurement()
	cmd := fmt.Sprintf("CREATE MEASUREMENT %s (tag1 TAG,tag2 TAG,tag3 TAG, field1 INT64 FIELD, field2 BOOL, field3 STRING, field4 FLOAT64)", measurement)
	_, err = c.Query(Query{Command: cmd, Database: databaseName})
	assert.Nil(t, err)
	tagFieldResult, err := c.ShowFieldKeys(databaseName, fmt.Sprintf("SHOW FIELD KEYS FROM %s", measurement))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(tagFieldResult))
	err = c.DropDatabase(databaseName)
	require.Nil(t, err)
}

func TestClient_DropMeasurement(t *testing.T) {
	c := testDefaultClient(t)

	err := c.DropMeasurement("", "test")
	require.Contains(t, err.Error(), "empty database name")

	databaseName := randomDatabaseName()
	err = c.DropMeasurement("test", "")
	require.Contains(t, err.Error(), "empty measurement name")

	err = c.CreateDatabase(databaseName)
	require.Nil(t, err)
	measurement := randomMeasurement()

	err = c.DropMeasurement(databaseName, measurement)
	require.Nil(t, err)

	p := &Point{}
	p.Measurement = measurement
	p.AddField("field", "test")
	err = c.WritePoint(databaseName, p, func(err error) {})
	assert.Nil(t, err)
	time.Sleep(time.Second * 3)
	err = c.DropMeasurement(databaseName, measurement)
	require.Nil(t, err)
}
