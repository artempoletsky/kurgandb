import { describe, expect, test, beforeAll, afterAll } from "@jest/globals";
import { Predicate, predicateToQuery } from "../src/client";
import { PlainObject, perfEnd, perfStart, perfDur, perfLog } from "../src/utils";
import { clientQueryUnsafe as clientQuery } from "../src/api";
import { before } from "node:test";
import { DataBase } from "../src/db";
import { PartitionMeta, Table } from "../src/table";
import { Document, FieldType } from "../src/document";


const xdescribe = (...args: any) => { };
const xtest = (...args: any) => { };

const TestTableName = "jest_test_table";

xdescribe("Table", () => {


  let t: Table<any>;
  beforeAll(() => {
    // DataBase.createTable({
    //   name: "posts",
    //   "fields": {
    //     "userid": "number",
    //     "title": "string",
    //     "text": "Text",
    //     "date": "date"
    //   },
    //   "settings": {}
    // });

    if (DataBase.isTableExist(TestTableName)) {
      DataBase.removeTable(TestTableName);
    }
    DataBase.createTable({
      name: TestTableName,
      fields: {
        name: "string",
        // light: "json",
        heavy: "JSON",
      }
    });
    t = new Table<any>(TestTableName);
  });

  test("adds a document", async () => {


    const idOffset = t.getLastIndex();
    const lenghtOffset = t.length;

    const d1 = await t.insert({
      name: "foo",
      heavy: {
        bar: Math.random()
      }
    });

    // d1.get("heavy")
    const d2 = await t.insert({
      name: "bar",
      heavy: {
        bar: Math.random()
      }
    });


    expect(t.length).toBe(lenghtOffset + 2);
    expect(d1.id).toBe(idOffset + 0);
    expect(d2.id).toBe(idOffset + 1);

    expect(t.getDocumentData(d1.getStringID())[0]).toBe("foo");
    expect(t.getDocumentData(d2.getStringID())[0]).toBe("bar");

  });

  test("renames a field", async () => {
    await t.renameField("heavy", "foo");
    // console.log(t.scheme.fields);
    expect(t).not.toHaveProperty("scheme.fields.heavy");
    expect(t).toHaveProperty("scheme.fields.foo");
  });


  test("removes a field", async () => {
    await t.removeField("foo");
    expect(t.scheme.fields).not.toHaveProperty("foo");
    expect((await t.at(0))?.name).toBe("foo");
  });

  test("adds a field", async () => {
    await t.addField("random", "number", e => Math.random());
    expect(t.scheme.fields).toHaveProperty("random");
    expect((await t.at(0))?.random).toBeLessThan(1);
  });


  xtest("selects from multiple partitions", async () => {
    perfStart("select");
    let docs = await t.select(doc => doc.random > 0.5);
    perfEnd("select");

    expect(perfDur("select")).toBeLessThan(3000);
    expect(docs.length).toBeGreaterThan(1);
    perfLog("select");
  });

  xtest("adds lorem ipsums", () => {
    // const measure = performance.measure("select");
    // let docs = t.select(doc => doc.random > 0.005);
    // expect(docs.length).toBeGreaterThan(4);
    // console.log(measure.duration);
    t.addField("lorem", "string", doc => `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec at quam venenatis dolor sagittis sodales. Fusce tristique efficitur libero, ut tincidunt nunc porttitor id. Integer sed felis est. In quis massa nibh. Etiam sed porttitor nisl. Aenean pretium ante purus, et elementum orci dapibus id. Nulla diam diam, viverra non elementum eu, mollis eget dui. Nam a nibh id sapien commodo volutpat. Integer condimentum ac velit eget ullamcorper. Sed dictum, mauris eget tincidunt rutrum, justo turpis hendrerit justo, quis sollicitudin odio metus lobortis enim. Pellentesque sodales ut quam a euismod. Sed vitae purus auctor, scelerisque leo faucibus, viverra sem. Aliquam hendrerit placerat lobortis. Cras id posuere nunc, sodales dapibus ex. Nulla quis pretium velit, in pulvinar ex.

    Maecenas lacinia porttitor leo. Etiam maximus, urna a aliquet pretium, est tellus efficitur erat, eu tristique eros augue vitae augue. Nullam placerat eu orci vel cursus. Pellentesque facilisis non libero at volutpat. Donec vestibulum tincidunt viverra. Nulla nec volutpat orci. Sed accumsan sollicitudin odio. Suspendisse potenti. Duis vitae quam nec magna efficitur condimentum ac nec sem. In porttitor maximus sollicitudin. Proin ultrices pellentesque imperdiet. Vestibulum nec nisl eu nisl dignissim dictum pharetra at diam. Suspendisse ac scelerisque tortor.
    
    Aenean eleifend nibh non sapien consectetur sagittis. Pellentesque laoreet lacus non luctus auctor. Vivamus congue nisi ac diam semper rutrum. Sed eget arcu convallis, facilisis augue ac, molestie turpis. Curabitur rutrum, arcu nec fermentum imperdiet, odio ante laoreet nisi, vitae fringilla nibh velit nec justo. Curabitur viverra urna eget massa porttitor, ac auctor orci lobortis. Ut pellentesque sit amet magna eget molestie. Sed vitae tristique lacus. Nullam convallis faucibus ipsum sit amet scelerisque. Sed maximus ex felis, ut ultricies arcu egestas sit amet. Fusce scelerisque urna blandit semper bibendum. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Aenean vehicula augue hendrerit volutpat aliquet. Proin tristique lobortis orci sed vestibulum. Fusce dui velit, pharetra pretium purus ut, mollis sollicitudin augue.
    
    Cras semper aliquam orci, ut consequat mauris imperdiet quis. Nam elit turpis, faucibus eleifend tellus sit amet, vehicula gravida massa. Fusce varius risus ac ante laoreet sodales. Cras ut elementum mauris. Fusce rhoncus sapien felis, at scelerisque eros egestas eu. Quisque dignissim dictum urna, nec feugiat lacus porta sit amet. Maecenas sapien ipsum, vestibulum id pulvinar viverra, facilisis ut tortor. Nunc a molestie diam. Cras rhoncus ipsum lorem, eu suscipit nisl finibus maximus. Pellentesque posuere eu augue eget viverra. Aenean finibus elementum augue id hendrerit. Vestibulum facilisis ligula non magna suscipit varius. Nunc leo diam, mattis in ipsum sit amet, ullamcorper ultricies risus. Nullam maximus sem sem, non fermentum erat viverra sit amet. Quisque lacinia aliquam ante, vel condimentum velit lacinia id. Maecenas malesuada massa mi, in finibus augue varius non.
    
    Duis sed quam aliquet, suscipit massa eget, porttitor nunc. Nam accumsan, risus et malesuada molestie, ex odio volutpat ante, eget porta mauris neque sed leo. Suspendisse imperdiet, felis et convallis gravida, urna nibh elementum risus, vitae accumsan erat libero in nibh. Vestibulum bibendum magna lectus, vitae vehicula turpis malesuada rutrum. Morbi dictum ornare sem, et tincidunt nunc dictum eu. Donec ac nibh aliquet, condimentum libero non, lacinia sapien. Morbi dapibus suscipit mattis. Pellentesque bibendum dolor ex, et sodales odio efficitur tincidunt. Duis egestas dui et ex viverra, eu commodo augue blandit. Proin ac auctor ligula. Mauris posuere, sem sed hendrerit aliquam, metus enim malesuada velit, ut lobortis est lorem vitae justo. Cras nibh velit, lobortis eu nisl sed, dapibus blandit leo. In at magna orci. Proin sit amet nisi nisl. Pellentesque placerat ultricies lectus, accumsan sollicitudin libero bibendum a.`);
  });

  afterAll(() => {
    t.closePartition();
    // DataBase.removeTable(tableName);
  });
});

describe("Rich table", () => {
  type RichType = {
    name: string
    lastTweet: string
    status: string
    isAdmin: boolean
    isModerator: boolean

    registrationDate: Date | string
    lastActive: Date | string
    birthday: Date | string

    password: string
    blog_posts: number[]
    favoriteQuote: string

    favoriteRandomNumber: number
  }

  const RichTypeFields = {
    "name": "string",
    "lastTweet": "string",
    "status": "string",
    "isAdmin": "boolean",
    "isModerator": "boolean",
    "registrationDate": "date",
    "lastActive": "date",
    "birthday": "date",
    "password": "password",
    "blog_posts": "json",
    "favoriteQuote": "string",
    "favoriteRandomNumber": "number",
  } satisfies Record<keyof RichType, FieldType>;

  const today = (new Date()).toJSON();
  const RichTypeRecord: RichType = {
    name: "John Doe",
    lastActive: today,
    birthday: today,
    registrationDate: today,
    favoriteQuote: "Poor and content is rich and rich enough",
    status: "The wolf is weaker than the lion and the tiger but it won't perform in the circus.",
    lastTweet: "The wolf is weaker than the lion and the tiger but it won't perform in the circus.",
    isAdmin: false,
    isModerator: false,
    password: "qwerty123",
    blog_posts: [343434, 234823742, 439785345, 34583475345, 3453845734, 3458334535, 1231248576],
    favoriteRandomNumber: 0,
  };
  let t: Table<RichType>;
  let row: any[];
  describe("filling", () => {

    beforeAll(() => {
      // DataBase.createTable({
      //   name: "users",
      //   "fields": {

      //   },
      //   "settings": {}
      // });

      if (DataBase.isTableExist(TestTableName)) {
        DataBase.removeTable(TestTableName);
      }

      DataBase.createTable({
        name: TestTableName,
        fields: RichTypeFields
      });

      t = new Table<RichType>(TestTableName);
      row = t.squarifyObject(RichTypeRecord);
    })

    test("Document.validate data ", () => {
      const invalidReason = Document.validateData(RichTypeRecord, t.scheme);
      expect(invalidReason).toBe(false)
    });

    test("fills multiple records", () => {
      const squaresToAdd = 1 * 1;

      const squareSize = 10 * 1000 * 1000;
      const square = Array.from(Array(squareSize)).map(() => row);

      expect(square.length).toBe(squareSize);
      expect(square[123][0]).toBe("John Doe");
      // type Rec = {
      //   random: number
      //   name: string
      // }
      const initialLenght = t.length;
      for (let i = 0; i < squaresToAdd; i++) {
        t.insertSquare(square);
      }
      expect(t.length).toBe(initialLenght + squaresToAdd * square.length);
    });

    
    xtest("ifdo", async () => {
      perfStart("ifdo");
      await t.ifDo(doc => doc.favoriteRandomNumber == 0, doc => doc.favoriteRandomNumber = Math.random());
      perfEnd("ifdo");
      perfLog("ifdo");
    });


    afterAll(() => {
      t.closePartition();
    })
  });



  describe("reading", () => {

    beforeAll(() => {
      t = new Table(TestTableName);
    })

    test("at", async () => {

      perfStart("query");
      // await clientQuery(async ({ jest_test_table }, { db }) => {
      //   return await jest_test_table.at(9);
      // });
      perfEnd("query");

      // console.log(await t.at(123123));
      
      perfStart("at");
      await t.at(9);
      perfEnd("at");

      perfLog("query");
      perfLog("at");
      expect(perfDur("query")).toBeLessThan(30)
      expect(perfDur("at")).toBeLessThan(30)

    });

    afterAll(() => {
      t.closePartition();
    })
  });

});


describe("Misc", () => {
  test("String id genetation consistancy", () => {
    const generate = (num: number) => {
      return Table.idNumber(Table.idString(num));
    }


    expect(generate(1)).toBe(1);
    expect(generate(1 * 100)).toBe(1 * 100);
    expect(generate(1000 * 1000)).toBe(1000 * 1000);
    expect(generate(10 * 1000 * 1000)).toBe(10 * 1000 * 1000);
    expect(generate(1000 * 1000 * 1000)).toBe(1000 * 1000 * 1000);
  });

  test("Partition ID search", () => {
    const partitions: PartitionMeta[] = [];
    const lenght = 1000 * 1000 * 1000;
    const partitionSize = 100 * 1000;
    const numPartitions = lenght / partitionSize;
    let end = partitionSize;
    for (let i = 0; i < numPartitions; i++) {
      partitions.push({
        length: partitionSize,
        end,
      });
      end += partitionSize;
    }

    const id = Math.floor(Math.random() * lenght);

    perfStart("partition search");
    const partitionId = Table.findPartitionForId(id, partitions, lenght);
    perfEnd("partition search");

    // perfLog("partition search")
    expect(partitionId).not.toBe(false);
    expect(perfDur("partition search")).toBeLessThan(20);
  });
});