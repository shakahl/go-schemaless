package client

import (
	"bytes"
	"context"
	"errors"

	"encoding/json"

	"github.com/rbastic/go-schemaless/examples/schemalessd/pkg/api"
	"github.com/rbastic/go-schemaless/models"

	"io/ioutil"
	"net/http"
	"strconv"
)

const contentTypeJSON = "application/json"

type Client struct {
	Address string
}

func New() *Client {
	return &Client{}
}

func (c *Client) WithAddress(addr string) *Client {
	c.Address = addr
	return c
}

func (c *Client) Get(ctx context.Context, storeName, tblName, rowKey, columnKey string, refKey int64) (cell models.Cell, found bool, err error) {
	postURL := c.Address + "/api/get"

	// TODO: make the context part of the request

	var getRequest api.GetRequest
	getRequest.Store = storeName
	getRequest.Table = tblName
	getRequest.RowKey = rowKey
	getRequest.ColumnKey = columnKey
	getRequest.RefKey = refKey

	getRequestMarshal, err := json.Marshal(getRequest)
	if err != nil {
		return models.Cell{}, false, err
	}

	request, err := http.NewRequest("POST", postURL, bytes.NewBuffer(getRequestMarshal))
	if err != nil {
		return models.Cell{}, false, err
	}
	request.Header.Set("Content-Type", contentTypeJSON)

	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return models.Cell{}, false, err
	}
	defer response.Body.Close()

	var responseBody []byte
	responseBody, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return models.Cell{}, false, err
	}
	var gr api.GetResponse

	err = json.Unmarshal(responseBody, &gr)
	if err != nil {
		return models.Cell{}, false, err
	}
	if gr.Error != "" {
		return models.Cell{}, false, errors.New(gr.Error)
	}

	return gr.Cell, gr.Found, nil
}

func (c *Client) GetLatest(ctx context.Context, storeName, tblName, rowKey, columnKey string) (cell models.Cell, found bool, err error) {
	postURL := c.Address + "/api/getLatest"

	var getLatestRequest api.GetRequest
	getLatestRequest.Store = storeName
	getLatestRequest.Table = tblName
	getLatestRequest.RowKey = rowKey
	getLatestRequest.ColumnKey = columnKey

	getLatestRequestMarshal, err := json.Marshal(getLatestRequest)
	if err != nil {
		return models.Cell{}, false, err
	}

	request, err := http.NewRequest("POST", postURL, bytes.NewBuffer(getLatestRequestMarshal))
	if err != nil {
		return models.Cell{}, false, err
	}
	request.Header.Set("Content-Type", contentTypeJSON)

	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return models.Cell{}, false, err
	}
	defer response.Body.Close()

	var responseBody []byte
	responseBody, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return models.Cell{}, false, err
	}

	var glr api.GetLatestResponse

	err = json.Unmarshal(responseBody, &glr)
	if err != nil {
		return models.Cell{}, false, err
	}
	if glr.Error != "" {
		return models.Cell{}, false, errors.New(glr.Error)
	}

	return glr.Cell, glr.Found, nil
}

func (c *Client) PartitionRead(ctx context.Context, storeName, tblName string, partitionNumber int, location string, value int64, limit int) (cells []models.Cell, found bool, err error) {
	postURL := c.Address + "/api/partitionRead"

	// TODO: add context

	var partitionReadRequest api.PartitionReadRequest
	partitionReadRequest.Store = storeName
	partitionReadRequest.Table = tblName
	partitionReadRequest.PartitionNumber = partitionNumber
	partitionReadRequest.Location = location
	partitionReadRequest.Value = strconv.FormatInt(value, 10)
	partitionReadRequest.Limit = limit

	partitionReadRequestMarshal, err := json.Marshal(partitionReadRequest)
	if err != nil {
		return nil, false, err
	}

	request, err := http.NewRequest("POST", postURL, bytes.NewBuffer(partitionReadRequestMarshal))
	if err != nil {
		return nil, false, err
	}
	request.Header.Set("Content-Type", contentTypeJSON)

	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return nil, false, err
	}
	defer response.Body.Close()

	var responseBody []byte
	responseBody, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, false, err
	}

	var prr api.PartitionReadResponse
	err = json.Unmarshal(responseBody, &prr)
	if err != nil {
		return nil, false, err
	}

	if prr.Error != "" {
		return nil, false, errors.New(prr.Error)
	}

	return prr.Cells, prr.Found, nil
}

func (c *Client) FindPartition(storeName, tblName, rowKey string) (*api.FindPartitionResponse, error) {
	postURL := c.Address + "/api/findPartition"

	var findRequest api.FindPartitionRequest
	findRequest.Store = storeName
	findRequest.Table = tblName
	findRequest.RowKey = rowKey

	findRequestMarshal, err := json.Marshal(findRequest)
	if err != nil {
		return nil, err
	}

	request, err := http.NewRequest("POST", postURL, bytes.NewBuffer(findRequestMarshal))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", contentTypeJSON)

	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	var responseBody []byte
	responseBody, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	fpr := new(api.FindPartitionResponse)
	err = json.Unmarshal(responseBody, fpr)
	if err != nil {
		return nil, err
	}

	if fpr.Error != "" {
		return nil, errors.New(fpr.Error)
	}

	return fpr, err
}

func (c *Client) Put(ctx context.Context, storeName, tblName, rowKey, columnKey string, refKey int64, body string) (*api.PutResponse, error) {
	postURL := c.Address + "/api/put"

	// TODO: make the context part of the request

	var putRequest api.PutRequest
	putRequest.Store = storeName
	putRequest.Table = tblName
	putRequest.RowKey = rowKey
	putRequest.ColumnKey = columnKey
	putRequest.RefKey = refKey
	putRequest.Body = body

	putRequestMarshal, err := json.Marshal(putRequest)
	if err != nil {
		return nil, err
	}

	request, err := http.NewRequest("POST", postURL, bytes.NewBuffer(putRequestMarshal))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", contentTypeJSON)

	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	var responseBody []byte
	responseBody, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	pr := new(api.PutResponse)
	err = json.Unmarshal(responseBody, pr)
	return pr, err
}
