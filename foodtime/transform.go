package main

import (
	"encoding/csv"
	"fmt"
	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform"
	"math"
	"strconv"
	"strings" 
)

const R = 6371.0

// doTransform is where you read the record that was written, and then you can
// return new records that will be written to the output topic
func doTransform(e transform.WriteEvent) ([]transform.Record, error) {
	// Skip empty records.
	if e.Record().Value == nil || len(e.Record().Value) == 0 {
		return []transform.Record{}, nil
	}

	data := string(e.Record().Value)

	// Parse the CSV data
	csvReader := csv.NewReader(strings.NewReader(data))
	record, err := csvReader.Read()
	if err != nil {
		fmt.Println("Error:", err)
		return nil, err
	}
	// Extract values from the CSV record
	restaurantLatitude := parseFloat(record[4])
	restaurantLongitude := parseFloat(record[5])
	deliveryLocationLatitude := parseFloat(record[6])
	deliveryLocationLongitude := parseFloat(record[7])

	// Calculate the distance between the two points
	distance := distCalculate(restaurantLatitude, restaurantLongitude, deliveryLocationLatitude, deliveryLocationLongitude)
	fmt.Println("Distance:", distance)

	// Create a new CSV record with the calculated distance
	resultRecord := []string{
		record[2],              // Keep the Age
		record[3],              // Keep the Rating
		fmt.Sprintf("%.2f", distance), // Add the calculated distance
		record[10],              // Keep the Times
	}

	// Convert the result record to a CSV string
	resultCSV := strings.Join(resultRecord, ",")

	// Convert the resultCSV string to []byte
	resultBytes := []byte(resultCSV)

	// Create a new transform.Record
	newRecord := transform.Record{
		Key:   e.Record().Key,
		Value: resultBytes,
	}

	return []transform.Record{newRecord}, nil
}

// parseFloat parses a float64 from a string and handles errors
func parseFloat(s string) float64 {
	val, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0.0
	}
	return val
}

// Function to convert degrees to radians
func degToRad(degrees float64) float64 {
	return degrees * (math.Pi / 180)
}

// Function to calculate the distance between two points using the haversine formula
func distCalculate(lat1, lon1, lat2, lon2 float64) float64 {
	dLat := degToRad(lat2 - lat1)
	dLon := degToRad(lon2 - lon1)
	a := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(degToRad(lat1))*math.Cos(degToRad(lat2))*math.Sin(dLon/2)*math.Sin(dLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return R * c
}

func main() {
	transform.OnRecordWritten(doTransform)
}
