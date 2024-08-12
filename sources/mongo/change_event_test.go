package mongo

import (
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"testing"
)

func TestNewChangeEvent(t *testing.T) {
	{
		// Insert
		objectID, err := primitive.ObjectIDFromHex("66a7f5fe65e80fad9c4773e7")
		assert.NoError(t, err)

		fullDocument := bson.M{
			"_id":        objectID,
			"email":      "wiptp.tbnnmfb@example.com",
			"first_name": "Wiptp",
			"last_name":  "Tbnnmfb",
		}

		changeEvent, err := NewChangeEvent(bson.M{
			"_id": bson.M{
				"_data": "8266A7F6000000008B2B042C0100296E5A1004778E391F64D84D65A154AA02089381C9463C6F7065726174696F6E54797065003C696E736572740046646F63756D656E744B65790046645F6964006466A7F5FE65E80FAD9C4773E7000004",
			},
			"clusterTime": bson.M{
				"ts": primitive.Timestamp{T: 1722283520, I: 139},
			},
			"documentKey": bson.M{
				"_id": objectID,
			},
			"fullDocument": fullDocument,
			"ns": bson.M{
				"coll": "customers",
				"db":   "inventory",
			},
			"operationType": "insert",
			"wallTime":      1722283520373,
		})

		assert.NoError(t, err)
		assert.NotNil(t, changeEvent)
		assert.Equal(t, "insert", changeEvent.operationType)
		assert.Equal(t, "customers", changeEvent.collection)
		assert.Equal(t, objectID, changeEvent.objectID)
		assert.Equal(t, fullDocument, *changeEvent.fullDocument)
		assert.Nil(t, changeEvent.fullDocumentBeforeChange)

		// ToMessage
		msg, err := changeEvent.ToMessage()
		assert.NoError(t, err)
		assert.Equal(t, map[string]interface{}{"id": `{"$oid":"66a7f5fe65e80fad9c4773e7"}`}, msg.pkMap)
		assert.Equal(t, "c", msg.operation)
		assert.Nil(t, msg.beforeJSONExtendedString)

		var actualDoc bson.M
		assert.NoError(t, bson.UnmarshalExtJSON([]byte(msg.afterJSONExtendedString), false, &actualDoc))
		assert.Equal(t, fullDocument, actualDoc)
	}
	{
		// Update
		for _, action := range []string{"update", "replace"} {
			objectID, err := primitive.ObjectIDFromHex("66834270bd422bc9b54b2be7")
			assert.NoError(t, err)

			fullDocument := bson.M{
				"_id":        objectID,
				"email":      "ilbwm.kzlhlza@example.com",
				"first_name": "Robin",
				"last_name":  "Kzlhlza",
			}

			rawMsg := bson.M{
				"_id": bson.M{
					"_data": "8266A7F95F000000072B042C0100296E5A1004778E391F64D84D65A154AA02089381C9463C6F7065726174696F6E54797065003C7570646174650046646F63756D656E744B65790046645F6964006466834270BD422BC9B54B2BE7000004",
				},
				"clusterTime": bson.M{
					"ts": primitive.Timestamp{T: 1722284383, I: 7},
				},
				"documentKey": bson.M{
					"_id": objectID,
				},
				"fullDocument": fullDocument,
				"ns": bson.M{
					"coll": "customers",
					"db":   "inventory",
				},
				"operationType": action,
				"updateDescription": bson.M{
					"removedFields":   []interface{}{},
					"truncatedArrays": []interface{}{},
					"updatedFields": bson.M{
						"first_name": "Robin",
					},
				},
				"wallTime": 1722284383120,
			}

			changeEvent, err := NewChangeEvent(rawMsg)
			assert.NoError(t, err)
			assert.NotNil(t, changeEvent)
			assert.Equal(t, action, changeEvent.operationType)
			assert.Equal(t, "customers", changeEvent.collection)
			assert.Equal(t, objectID, changeEvent.objectID)
			assert.Equal(t, fullDocument, *changeEvent.fullDocument)
			assert.Nil(t, changeEvent.fullDocumentBeforeChange)

			// ToMessage
			msg, err := changeEvent.ToMessage()
			assert.NoError(t, err)
			assert.Equal(t, map[string]interface{}{"id": `{"$oid":"66834270bd422bc9b54b2be7"}`}, msg.pkMap)
			assert.Equal(t, "u", msg.operation)

			var expectedObj bson.M
			assert.NoError(t, bson.UnmarshalExtJSON([]byte(msg.afterJSONExtendedString), false, &expectedObj))
			assert.Equal(t, fullDocument, expectedObj)

			{
				// Edge case with update where `fullDocument` is present, but it's null
				rawMsg["fullDocument"] = nil
				changeEvent, err = NewChangeEvent(rawMsg)
				assert.NoError(t, err)
				assert.NotNil(t, changeEvent.fullDocument)
				assert.Equal(t, bson.M{"_id": objectID}, *changeEvent.fullDocument)
			}
		}
	}
	{
		// Update (w fullDocumentBeforeChange)
		for _, action := range []string{"update", "replace"} {
			objectID, err := primitive.ObjectIDFromHex("66834270bd422bc9b54b2be7")
			assert.NoError(t, err)

			beforeFullDoc := bson.M{
				"_id":        objectID,
				"email":      "ilbwm.kzlhlza@example.com",
				"first_name": "Old Robin",
				"last_name":  "Old Kzlhlza",
			}

			fullDocument := bson.M{
				"_id":        objectID,
				"email":      "ilbwm.kzlhlza@example.com",
				"first_name": "Robin",
				"last_name":  "Kzlhlza",
			}

			rawMsg := bson.M{
				"_id": bson.M{
					"_data": "8266A7F95F000000072B042C0100296E5A1004778E391F64D84D65A154AA02089381C9463C6F7065726174696F6E54797065003C7570646174650046646F63756D656E744B65790046645F6964006466834270BD422BC9B54B2BE7000004",
				},
				"clusterTime": bson.M{
					"ts": primitive.Timestamp{T: 1722284383, I: 7},
				},
				"documentKey": bson.M{
					"_id": objectID,
				},
				"fullDocument":             fullDocument,
				"fullDocumentBeforeChange": beforeFullDoc,
				"ns": bson.M{
					"coll": "customers",
					"db":   "inventory",
				},
				"operationType": action,
				"updateDescription": bson.M{
					"removedFields":   []interface{}{},
					"truncatedArrays": []interface{}{},
					"updatedFields": bson.M{
						"first_name": "Robin",
					},
				},
				"wallTime": 1722284383120,
			}

			changeEvent, err := NewChangeEvent(rawMsg)
			assert.NoError(t, err)
			assert.NotNil(t, changeEvent)
			assert.Equal(t, action, changeEvent.operationType)
			assert.Equal(t, "customers", changeEvent.collection)
			assert.Equal(t, objectID, changeEvent.objectID)
			assert.Equal(t, fullDocument, *changeEvent.fullDocument)
			assert.Equal(t, beforeFullDoc, *changeEvent.fullDocumentBeforeChange)

			// ToMessage
			msg, err := changeEvent.ToMessage()
			assert.NoError(t, err)
			assert.Equal(t, map[string]interface{}{"id": `{"$oid":"66834270bd422bc9b54b2be7"}`}, msg.pkMap)
			assert.Equal(t, "u", msg.operation)

			{
				// Full Document
				var actualDoc bson.M
				assert.NoError(t, bson.UnmarshalExtJSON([]byte(msg.afterJSONExtendedString), false, &actualDoc))
				assert.Equal(t, fullDocument, actualDoc)
			}
			{
				// Before Full Document
				var actualBeforeDoc bson.M
				assert.NoError(t, bson.UnmarshalExtJSON([]byte(*msg.beforeJSONExtendedString), false, &actualBeforeDoc))
				assert.Equal(t, beforeFullDoc, actualBeforeDoc)
			}

			{
				// Edge case with update where `fullDocument` is present, but it's null
				rawMsg["fullDocument"] = nil
				changeEvent, err = NewChangeEvent(rawMsg)
				assert.NoError(t, err)
				assert.NotNil(t, changeEvent.fullDocument)
				assert.Equal(t, bson.M{"_id": objectID}, *changeEvent.fullDocument)
			}
		}
	}
	{
		// Delete
		objectID, err := primitive.ObjectIDFromHex("66834270bd422bc9b54b2be6")
		assert.NoError(t, err)

		changeEvent, err := NewChangeEvent(bson.M{
			"_id": bson.M{
				"_data": "8266A7F8BB000000062B042C0100296E5A1004778E391F64D84D65A154AA02089381C9463C6F7065726174696F6E54797065003C64656C6574650046646F63756D656E744B65790046645F6964006466834270BD422BC9B54B2BE6000004",
			},
			"clusterTime": bson.M{
				"ts": primitive.Timestamp{T: 1722284219, I: 6},
			},
			"documentKey": bson.M{
				"_id": objectID,
			},
			"ns": bson.M{
				"coll": "customers",
				"db":   "inventory",
			},
			"operationType": "delete",
			"wallTime":      1722284219184,
		})

		assert.NoError(t, err)
		assert.NotNil(t, changeEvent)
		assert.Equal(t, "delete", changeEvent.operationType)
		assert.Equal(t, "customers", changeEvent.collection)
		assert.Equal(t, objectID, changeEvent.objectID)
		assert.Nil(t, changeEvent.fullDocument)
		assert.Nil(t, changeEvent.fullDocumentBeforeChange)

		// ToMessage
		msg, err := changeEvent.ToMessage()
		assert.NoError(t, err)
		assert.Equal(t, map[string]interface{}{"id": `{"$oid":"66834270bd422bc9b54b2be6"}`}, msg.pkMap)
		assert.Equal(t, "d", msg.operation)
		assert.Equal(t, `{"_id":{"$oid":"66834270bd422bc9b54b2be6"}}`, msg.afterJSONExtendedString)
		assert.Nil(t, msg.beforeJSONExtendedString)
	}
	{
		// Delete (w fullDocumentBeforeChange)
		objectID, err := primitive.ObjectIDFromHex("66834270bd422bc9b54b2be6")
		assert.NoError(t, err)

		fullDocument := bson.M{
			"_id":        objectID,
			"email":      "dusty@artie.com",
			"first_name": "Dusty",
			"last_name":  "The mini aussie",
		}

		changeEvent, err := NewChangeEvent(bson.M{
			"_id": bson.M{
				"_data": "8266A7F8BB000000062B042C0100296E5A1004778E391F64D84D65A154AA02089381C9463C6F7065726174696F6E54797065003C64656C6574650046646F63756D656E744B65790046645F6964006466834270BD422BC9B54B2BE6000004",
			},
			"clusterTime": bson.M{
				"ts": primitive.Timestamp{T: 1722284219, I: 6},
			},
			"documentKey": bson.M{
				"_id": objectID,
			},
			"fullDocumentBeforeChange": fullDocument,
			"ns": bson.M{
				"coll": "customers",
				"db":   "inventory",
			},
			"operationType": "delete",
			"wallTime":      1722284219184,
		})

		assert.NoError(t, err)
		assert.NotNil(t, changeEvent)
		assert.Equal(t, "delete", changeEvent.operationType)
		assert.Equal(t, "customers", changeEvent.collection)
		assert.Equal(t, objectID, changeEvent.objectID)
		assert.Nil(t, changeEvent.fullDocument)
		assert.Equal(t, fullDocument, *changeEvent.fullDocumentBeforeChange)

		// ToMessage
		msg, err := changeEvent.ToMessage()
		assert.NoError(t, err)
		assert.Equal(t, map[string]interface{}{"id": `{"$oid":"66834270bd422bc9b54b2be6"}`}, msg.pkMap)
		assert.Equal(t, "d", msg.operation)
		assert.Equal(t, `{"_id":{"$oid":"66834270bd422bc9b54b2be6"}}`, msg.afterJSONExtendedString)

		var actualBeforeDoc bson.M
		assert.NoError(t, bson.UnmarshalExtJSON([]byte(*msg.beforeJSONExtendedString), false, &actualBeforeDoc))
		assert.Equal(t, fullDocument, actualBeforeDoc)
	}
}
