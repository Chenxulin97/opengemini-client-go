package opengemini

import (
	"errors"
	"io"
	"net/http"
	"net/url"
)

type ValuesResult struct {
	Measurement string
	Values      []interface{}
}

func (c *client) ShowTagKeys(database, command string) ([]ValuesResult, error) {
	if len(database) == 0 {
		return nil, errors.New("empty database name")
	}
	tagKeyResult, err := c.showTagSeriesQuery(database, command)
	if err != nil {
		return nil, err
	}
	return tagKeyResult, nil
}

func (c *client) ShowTagValues(database, command string) ([]ValuesResult, error) {
	if len(database) == 0 {
		return nil, errors.New("empty database name")
	}
	if len(command) == 0 {
		return nil, errors.New("empty command")
	}

	tagValueResult, err := c.showTagFieldQuery(database, command)
	if err != nil {
		return nil, err
	}
	return tagValueResult, nil
}

func (c *client) ShowFieldKeys(database, command string) ([]ValuesResult, error) {
	if len(database) == 0 {
		return nil, errors.New("empty database name")
	}

	if len(command) == 0 {
		return nil, errors.New("empty command")
	}

	tagKeyResult, err := c.showTagFieldQuery(database, command)
	if err != nil {
		return nil, err
	}
	return tagKeyResult, nil
}

func (c *client) ShowSeries(database, command string) ([]string, error) {
	if len(database) == 0 {
		return nil, errors.New("empty database name")
	}
	seriesResult, err := c.showTagSeriesQuery(database, command)
	if err != nil {
		return nil, err
	}
	if len(seriesResult) == 0 {
		return []string{}, nil
	}
	var (
		values = seriesResult[0].Values
		series = make([]string, 0, len(values))
	)
	for _, v := range values {
		strV, ok := v.(string)
		if !ok {
			return series, nil
		}
		series = append(series, strV)
	}
	return series, nil
}

func (c *client) DropMeasurement(database, measurement string) error {
	if len(database) == 0 {
		return errors.New("empty database name")
	}
	if len(measurement) == 0 {
		return errors.New("empty measurement name")
	}
	req := requestDetails{
		queryValues: make(url.Values),
	}
	req.queryValues.Add("db", database)
	req.queryValues.Add("q", "DROP MEASUREMENT \""+measurement+"\"")
	resp, err := c.executeHttpPost("/query", req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.New("read resp failed, error: " + err.Error())
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New("error resp, code: " + resp.Status + "body: " + string(body))
	}
	return nil
}
