import { describe, expect, test, beforeAll, afterAll } from "@jest/globals";
import { Predicate, predicateToQuery } from "../src/client";
import { PlainObject } from "../src/utils";
import { clientQueryUnsafe as clientQuery } from "../src/api";
import { before } from "node:test";
import { DataBase } from "../src/db";
import { Table } from "../src/table";


const xdescribe = (...args: any) => { };
const xtest = (...args: any) => { };


xdescribe("Table", () => {
  const tableName = "jest_test_table";
  let t: Table;
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

    if (DataBase.isTableExist(tableName)) {
      DataBase.removeTable(tableName);
    }
    DataBase.createTable({
      name: tableName,
      fields: {
        name: "string",
        // light: "json",
        heavy: "JSON",
      }
    });
    t = new Table(tableName);
  });

  test("adds a document", () => {


    const idOffset = t.getLastIndex();
    const lenghtOffset = t.length;

    const d1 = t.insert({
      name: "foo",
      heavy: {
        bar: Math.random()
      }
    });

    const d2 = t.insert({
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

  test("renames a field", () => {
    t.renameField("heavy", "foo");
    // console.log(t.scheme.fields);
    expect(t).not.toHaveProperty("scheme.fields.heavy");
    expect(t).toHaveProperty("scheme.fields.foo");
  });


  test("removes a field", () => {
    t.removeField("foo");
    expect(t.scheme.fields).not.toHaveProperty("foo");
    expect(t.at(0)?.name).toBe("foo");
  });

  test("adds a field", () => {
    t.addField("random", "number", e => Math.random());
    expect(t.scheme.fields).toHaveProperty("random");
    expect(t.at(0)?.random).toBeLessThan(1);
  });


  test("fills multiple records", () => {
    const docsToAdd = 1000000;
    type Rec = {
      random: number
      name: string
    }
    const initialLenght = t.length;
    for (let i = 0; i < docsToAdd; i++) {
      t.insert<Rec>({
        random: Math.random(),
        name: "asdasd",
      });
    }
    expect(t.length).toBe(initialLenght + docsToAdd);
  });

  test("selects from multiple partitions", () => {
    // const measure = performance.measure("select");
    let docs = t.select(doc => doc.random > 0.005);
    // expect(docs.length).toBeGreaterThan(4);
    // console.log(measure.duration);
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

    performance.mark("query_start");
    await clientQuery(({ jest_test_table }, { db }) => {
      return jest_test_table.at(-1);
    });
    performance.mark("tableCtor_start");
    const t = new Table("jest_test_table");
    performance.mark("tableCtor_end");

    performance.mark("at_start");
    t.at(100423);
    performance.mark("at_end");

    performance.mark("query_end");
    // expect(measure.duration).toBeLessThan(1);
    performance.measure("query", "query_start", "query_end");
    performance.measure("at", "at_start", "at_end");
    performance.measure("tableCtor", "tableCtor_start", "tableCtor_end");
    //  const measure =
    const duration = performance.getEntriesByName("query")[0].duration;
    // console.log(duration);
    // console.log(performance.getEntriesByName("at")[0].duration);
  });
});