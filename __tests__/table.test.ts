import { describe, expect, test, beforeAll, afterAll } from "@jest/globals";
import { Predicate, predicateToQuery } from "../src/client";
import { PlainObject, perfEnd, perfStart, perfDur, perfLog } from "../src/utils";
import { clientQueryUnsafe as clientQuery } from "../src/api";
import { before } from "node:test";
import { DataBase } from "../src/db";
import { Table } from "../src/table";


const xdescribe = (...args: any) => { };
const xtest = (...args: any) => { };

const TestTableName = "jest_test_table";

describe("Table", () => {
  

  let t: Table<any>;
  beforeAll(() => {

    // DataBase.createTable({
    //   name: "users",
    //   "fields": {
    //     "name": "string",
    //     "isAdmin": "boolean",
    //     "isModerator": "boolean",
    //     "registrationDate": "date",
    //     "posts": "JSON",
    //     "password": "password",
    //     "blog_posts": "json"
    //   },
    //   "settings": {}
    // });
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


  xtest("fills multiple records", () => {
    const docsToAdd = 1000 * 1000;
    // type Rec = {
    //   random: number
    //   name: string
    // }
    const initialLenght = t.length;
    for (let i = 0; i < docsToAdd; i++) {
      t.insert({
        random: Math.random(),
        name: "asdasd",
      });
    }
    expect(t.length).toBe(initialLenght + docsToAdd);
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

describe("Heavy table", () => {
  test("selects by id", async () => {

    perfStart("query");
    await clientQuery(({ jest_test_table }, { db }) => {
      return jest_test_table.at(1123);
    });
    perfEnd("query");

    const t = new Table("jest_test_table");

    perfStart("at");
    t.at(100423);
    perfEnd("at");

    perfLog("query");
    expect(perfDur("query")).toBeLessThan(1)
    expect(perfDur("at")).toBeLessThan(1)

  });
});