using Polecat.Patching;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Patching;

[Collection("integration")]
public class patching_api : IntegrationContext
{
    public patching_api(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async Task InitializeAsync()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "patching"; });
    }

    // ---- Set operations ----

    [Fact]
    public async Task set_an_immediate_property_by_id()
    {
        var target = Target.Random();
        target.Number = 5;

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Set(x => x.Number, 10);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Target>(target.Id))!.Number.ShouldBe(10);
    }

    [Fact]
    public async Task set_a_deep_property_by_id()
    {
        var target = Target.Random(true);
        target.Inner!.Number = 5;

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Set(x => x.Inner!.Number, 10);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Target>(target.Id))!.Inner!.Number.ShouldBe(10);
    }

    [Fact]
    public async Task set_an_immediate_property_by_where_clause()
    {
        var target1 = new Target { Id = Guid.NewGuid(), Color = "Blue", Number = 1 };
        var target2 = new Target { Id = Guid.NewGuid(), Color = "Blue", Number = 1 };
        var target3 = new Target { Id = Guid.NewGuid(), Color = "Blue", Number = 1 };
        var target4 = new Target { Id = Guid.NewGuid(), Color = "Green", Number = 1 };
        var target5 = new Target { Id = Guid.NewGuid(), Color = "Green", Number = 1 };
        var target6 = new Target { Id = Guid.NewGuid(), Color = "Red", Number = 1 };

        theSession.Store(target1, target2, target3, target4, target5, target6);
        await theSession.SaveChangesAsync();

        // Change every Target document where the Color is Blue
        theSession.Patch<Target>(x => x.Color == "Blue").Set(x => x.Number, 2);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        // These should have been updated
        (await query.LoadAsync<Target>(target1.Id))!.Number.ShouldBe(2);
        (await query.LoadAsync<Target>(target2.Id))!.Number.ShouldBe(2);
        (await query.LoadAsync<Target>(target3.Id))!.Number.ShouldBe(2);

        // These should not because they didn't match the where clause
        (await query.LoadAsync<Target>(target4.Id))!.Number.ShouldBe(1);
        (await query.LoadAsync<Target>(target5.Id))!.Number.ShouldBe(1);
        (await query.LoadAsync<Target>(target6.Id))!.Number.ShouldBe(1);
    }

    [Fact]
    public async Task set_a_string_property_by_name()
    {
        var target = Target.Random();
        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Set("String", "updated");
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Target>(target.Id))!.String.ShouldBe("updated");
    }

    [Fact]
    public async Task set_nested_property_by_name_and_parent()
    {
        var target = Target.Random(true);
        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Set<Target?, string>("String", x => x.Inner, "nested_val");
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Target>(target.Id))!.Inner!.String.ShouldBe("nested_val");
    }

    // ---- Duplicate operations ----

    [Fact]
    public async Task duplicate_to_new_field()
    {
        var target = Target.Random();
        target.AnotherString = null;
        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Duplicate(t => t.String, t => t.AnotherString);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var result = await query.LoadAsync<Target>(target.Id);
        result!.AnotherString.ShouldBe(target.String);
    }

    [Fact]
    public async Task duplicate_to_multiple_new_fields()
    {
        var target = Target.Random();
        target.StringField = null;
        target.Inner = null;
        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Duplicate(t => t.String,
            t => t.StringField,
            t => t.AnotherString);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var result = await query.LoadAsync<Target>(target.Id);
        result!.StringField.ShouldBe(target.String);
        result.AnotherString.ShouldBe(target.String);
    }

    // ---- Increment operations ----

    [Fact]
    public async Task increment_for_int()
    {
        var target = Target.Random();
        target.Number = 6;

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Increment(x => x.Number);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Target>(target.Id))!.Number.ShouldBe(7);
    }

    [Fact]
    public async Task increment_for_int_with_explicit_increment()
    {
        var target = Target.Random();
        target.Number = 6;

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Increment(x => x.Number, 3);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Target>(target.Id))!.Number.ShouldBe(9);
    }

    [Fact]
    public async Task increment_for_long()
    {
        var target = Target.Random();
        target.Long = 13;

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Increment(x => x.Long);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Target>(target.Id))!.Long.ShouldBe(14);
    }

    [Fact]
    public async Task increment_for_double()
    {
        var target = Target.Random();
        target.Double = 11.2;

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Increment(x => x.Double, 2.4);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var result = (await query.LoadAsync<Target>(target.Id))!.Double;
        result.ShouldBe(13.6, 0.01);
    }

    [Fact]
    public async Task increment_for_float()
    {
        var target = Target.Random();
        target.Float = 11.2F;

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Increment(x => x.Float, 2.4F);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var result = (await query.LoadAsync<Target>(target.Id))!.Float;
        result.ShouldBe(13.6F, 0.1F);
    }

    [Fact]
    public async Task increment_for_decimal()
    {
        var target = Target.Random();
        target.Decimal = 11.2m;

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Increment(x => x.Decimal, 2.4m);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var result = (await query.LoadAsync<Target>(target.Id))!.Decimal;
        Math.Round(result, 1).ShouldBe(13.6m);
    }

    [Fact]
    public async Task increment_for_int_from_dictionary()
    {
        var target = Target.Random();
        target.NumberByKey["whatever"] = 6;

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Increment(x => x.NumberByKey["whatever"]);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Target>(target.Id))!.NumberByKey["whatever"].ShouldBe(7);
    }

    [Fact]
    public async Task increment_for_int_with_explicit_increment_from_dictionary()
    {
        var target = Target.Random();
        target.NumberByKey["whatever"] = 6;

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Increment(x => x.NumberByKey["whatever"], 3);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Target>(target.Id))!.NumberByKey["whatever"].ShouldBe(9);
    }

    [Fact]
    public async Task increment_for_long_from_dictionary()
    {
        var target = Target.Random();
        target.LongByKey["whatever"] = 13;

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Increment(x => x.LongByKey["whatever"]);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Target>(target.Id))!.LongByKey["whatever"].ShouldBe(14);
    }

    [Fact]
    public async Task increment_for_double_from_dictionary()
    {
        var target = Target.Random();
        target.DoubleByKey["whatever"] = 11.2;

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Increment(x => x.DoubleByKey["whatever"], 2.4);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var result = (await query.LoadAsync<Target>(target.Id))!.DoubleByKey["whatever"];
        result.ShouldBe(13.6, 0.01);
    }

    [Fact]
    public async Task increment_for_float_from_dictionary()
    {
        var target = Target.Random();
        target.FloatByKey["whatever"] = 11.2F;

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Increment(x => x.FloatByKey["whatever"], 2.4F);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var result = (await query.LoadAsync<Target>(target.Id))!.FloatByKey["whatever"];
        result.ShouldBe(13.6F, 0.1F);
    }

    [Fact]
    public async Task increment_for_decimal_from_dictionary()
    {
        var target = Target.Random();
        target.DecimalByKey["whatever"] = 11.2m;

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Increment(x => x.DecimalByKey["whatever"], 2.4m);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var result = (await query.LoadAsync<Target>(target.Id))!.DecimalByKey["whatever"];
        Math.Round(result, 1).ShouldBe(13.6m);
    }

    // ---- Append operations ----

    [Fact]
    public async Task append_to_a_primitive_array()
    {
        var target = Target.Random();
        target.NumberArray = [1, 2, 3];

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Append(x => x.NumberArray, 4);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Target>(target.Id))!.NumberArray.ShouldBe([1, 2, 3, 4]);
    }

    [Fact]
    public async Task append_if_not_exists_to_a_primitive_array()
    {
        var target = Target.Random();
        target.NumberArray = [1, 2, 3];

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        // Should not add 3 since it already exists
        theSession.Patch<Target>(target.Id).AppendIfNotExists(x => x.NumberArray, 3);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Target>(target.Id))!.NumberArray.ShouldBe([1, 2, 3]);

        // Should add 4 since it does not exist
        await using var session2 = theStore.LightweightSession();
        session2.Patch<Target>(target.Id).AppendIfNotExists(x => x.NumberArray, 4);
        await session2.SaveChangesAsync();

        await using var query2 = theStore.QuerySession();
        (await query2.LoadAsync<Target>(target.Id))!.NumberArray.ShouldBe([1, 2, 3, 4]);
    }

    [Fact]
    public async Task append_complex_element()
    {
        var target = Target.Random(true);
        var initialCount = target.Children.Length;

        var child = Target.Random();

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Append(x => x.Children, child);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var target2 = await query.LoadAsync<Target>(target.Id);
        target2!.Children.Length.ShouldBe(initialCount + 1);
        target2.Children.Last().Id.ShouldBe(child.Id);
    }

    [Fact]
    public async Task append_complex_element_key_value()
    {
        var target = Target.Random(true);
        var initialCount = target.ChildrenDictionary.Count;

        var key = "whatever";
        var child = Target.Random();

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Append(x => x.ChildrenDictionary, key, child);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var target2 = await query.LoadAsync<Target>(target.Id);
        target2!.ChildrenDictionary.Count.ShouldBe(initialCount + 1);
        target2.ChildrenDictionary.ShouldContainKey(key);
        target2.ChildrenDictionary[key].Id.ShouldBe(child.Id);
    }

    [Fact]
    public async Task append_if_not_exists_complex_element()
    {
        var target = Target.Random(true);
        var initialCount = target.Children.Length;

        var child = Target.Random();
        var child2 = Target.Random();

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        // Append child first
        theSession.Patch<Target>(target.Id).Append(x => x.Children, child);
        await theSession.SaveChangesAsync();

        // Append same child again with AppendIfNotExists - should not add
        await using var session2 = theStore.LightweightSession();
        session2.Patch<Target>(target.Id).AppendIfNotExists(x => x.Children, child);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var target2 = await query.LoadAsync<Target>(target.Id);
        target2!.Children.Length.ShouldBe(initialCount + 1);
        target2.Children.Last().Id.ShouldBe(child.Id);

        // Append different child - should add
        await using var session3 = theStore.LightweightSession();
        session3.Patch<Target>(target.Id).AppendIfNotExists(x => x.Children, child2);
        await session3.SaveChangesAsync();

        await using var query2 = theStore.QuerySession();
        var target3 = await query2.LoadAsync<Target>(target.Id);
        target3!.Children.Length.ShouldBe(initialCount + 2);
        target3.Children.Last().Id.ShouldBe(child2.Id);
    }

    [Fact]
    public async Task append_if_not_exists_complex_element_key_value()
    {
        var target = Target.Random(true);
        var initialCount = target.ChildrenDictionary.Count;

        var child = Target.Random();
        var child2 = Target.Random();

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Append(x => x.ChildrenDictionary, "value1", child);
        await theSession.SaveChangesAsync();

        // Should not re-add "value1"
        await using var session2 = theStore.LightweightSession();
        session2.Patch<Target>(target.Id).AppendIfNotExists(x => x.ChildrenDictionary, "value1", child);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var target2 = await query.LoadAsync<Target>(target.Id);
        target2!.ChildrenDictionary.Count.ShouldBe(initialCount + 1);
        target2.ChildrenDictionary.ShouldContainKey("value1");

        // Should add "value2"
        await using var session3 = theStore.LightweightSession();
        session3.Patch<Target>(target.Id).AppendIfNotExists(x => x.ChildrenDictionary, "value2", child2);
        await session3.SaveChangesAsync();

        await using var query2 = theStore.QuerySession();
        var target3 = await query2.LoadAsync<Target>(target.Id);
        target3!.ChildrenDictionary.Count.ShouldBe(initialCount + 2);
        target3.ChildrenDictionary["value2"].Id.ShouldBe(child2.Id);
    }

    // ---- Insert operations ----

    [Fact]
    public async Task insert_to_a_primitive_array()
    {
        var target = Target.Random();
        target.NumberArray = [1, 2, 3];

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Insert(x => x.NumberArray, 4);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Target>(target.Id))!.NumberArray.ShouldBe([1, 2, 3, 4]);
    }

    [Fact]
    public async Task insert_to_a_primitive_array_at_a_certain_position()
    {
        var target = Target.Random();
        target.NumberArray = [1, 2, 3];

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Insert(x => x.NumberArray, 4, 2);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Target>(target.Id))!.NumberArray.ShouldBe([1, 2, 4, 3]);
    }

    [Fact]
    public async Task insert_if_not_exists_last_to_a_primitive_array()
    {
        var target = Target.Random();
        target.NumberArray = [1, 2, 3];

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        // Should not add 1 since it exists
        theSession.Patch<Target>(target.Id).InsertIfNotExists(x => x.NumberArray, 1);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Target>(target.Id))!.NumberArray.ShouldBe([1, 2, 3]);

        // Should add 4 since it doesn't exist
        await using var session2 = theStore.LightweightSession();
        session2.Patch<Target>(target.Id).InsertIfNotExists(x => x.NumberArray, 4);
        await session2.SaveChangesAsync();

        await using var query2 = theStore.QuerySession();
        (await query2.LoadAsync<Target>(target.Id))!.NumberArray.ShouldBe([1, 2, 3, 4]);
    }

    [Fact]
    public async Task insert_if_not_exists_at_a_certain_position()
    {
        var target = Target.Random();
        target.NumberArray = [1, 2, 3];

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        // Should not add 3 since it already exists
        theSession.Patch<Target>(target.Id).InsertIfNotExists(x => x.NumberArray, 3, 2);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Target>(target.Id))!.NumberArray.ShouldBe([1, 2, 3]);

        // Should add 4 at position 2
        await using var session2 = theStore.LightweightSession();
        session2.Patch<Target>(target.Id).InsertIfNotExists(x => x.NumberArray, 4, 2);
        await session2.SaveChangesAsync();

        await using var query2 = theStore.QuerySession();
        (await query2.LoadAsync<Target>(target.Id))!.NumberArray.ShouldBe([1, 2, 4, 3]);
    }

    [Fact]
    public async Task insert_complex_element()
    {
        var target = Target.Random(true);
        var initialCount = target.Children.Length;

        var child = Target.Random();

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Insert(x => x.Children, child);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var target2 = await query.LoadAsync<Target>(target.Id);
        target2!.Children.Length.ShouldBe(initialCount + 1);
        target2.Children.Last().Id.ShouldBe(child.Id);
    }

    [Fact]
    public async Task insert_if_not_exists_last_complex_element()
    {
        var target = Target.Random(true);
        var initialCount = target.Children.Length;

        var child = Target.Random();
        var child2 = Target.Random();
        theSession.Store(target);
        await theSession.SaveChangesAsync();

        // Insert child
        theSession.Patch<Target>(target.Id).Insert(x => x.Children, child);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var target2 = await query.LoadAsync<Target>(target.Id);
        target2!.Children.Length.ShouldBe(initialCount + 1);
        target2.Children.Last().Id.ShouldBe(child.Id);

        // InsertIfNotExists with same child - should not add
        await using var session2 = theStore.LightweightSession();
        session2.Patch<Target>(target.Id).InsertIfNotExists(x => x.Children, child);
        await session2.SaveChangesAsync();

        await using var query2 = theStore.QuerySession();
        var target3 = await query2.LoadAsync<Target>(target.Id);
        target3!.Children.Length.ShouldBe(initialCount + 1);

        // InsertIfNotExists with different child - should add
        await using var session3 = theStore.LightweightSession();
        session3.Patch<Target>(target.Id).InsertIfNotExists(x => x.Children, child2);
        await session3.SaveChangesAsync();

        await using var query3 = theStore.QuerySession();
        var target4 = await query3.LoadAsync<Target>(target.Id);
        target4!.Children.Length.ShouldBe(initialCount + 2);
        target4.Children.Last().Id.ShouldBe(child2.Id);
    }

    // ---- Rename operations ----

    [Fact]
    public async Task rename_shallow_prop()
    {
        var target = Target.Random(true);
        target.String = "Foo";
        target.AnotherString = "Bar";

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Rename("String", x => x.AnotherString);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var target2 = await query.LoadAsync<Target>(target.Id);
        target2!.AnotherString.ShouldBe("Foo");
        target2.String.ShouldBeNull();
    }

    [Fact]
    public async Task rename_deep_prop()
    {
        var target = Target.Random(true);
        target.Inner!.String = "Foo";
        target.Inner.AnotherString = "Bar";

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Rename("String", x => x.Inner!.AnotherString);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var target2 = await query.LoadAsync<Target>(target.Id);
        target2!.Inner!.AnotherString.ShouldBe("Foo");
        target2.Inner.String.ShouldBeNull();
    }

    // ---- Remove operations ----

    [Fact]
    public async Task remove_primitive_element()
    {
        var target = Target.Random();
        target.NumberArray = [1, 2, 3];

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Remove(x => x.NumberArray, 2);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var target2 = await query.LoadAsync<Target>(target.Id);
        target2!.NumberArray.Length.ShouldBe(2);
        target2.NumberArray.ShouldBe([1, 3]);
    }

    [Fact]
    public async Task remove_repeated_primitive_elements()
    {
        var target = Target.Random();
        target.NumberArray = [1, 2, 3, 2];

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Remove(x => x.NumberArray, 2, RemoveAction.RemoveAll);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var target2 = await query.LoadAsync<Target>(target.Id);
        target2!.NumberArray.Length.ShouldBe(2);
        target2.NumberArray.ShouldBe([1, 3]);
    }

    [Fact]
    public async Task remove_complex_element()
    {
        var target = Target.Random(true);
        var initialCount = target.Children.Length;
        var child = target.Children[0];

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Remove(x => x.Children, child);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var target2 = await query.LoadAsync<Target>(target.Id);
        target2!.Children.Length.ShouldBe(initialCount - 1);
        target2.Children.ShouldNotContain(t => t.Id == child.Id);
    }

    [Fact]
    public async Task remove_complex_element_key()
    {
        var target = Target.Random(true);

        var removedKey = "whatever";
        target.ChildrenDictionary.Add(removedKey, Target.Random());
        var initialCount = target.ChildrenDictionary.Count;

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Remove(x => x.ChildrenDictionary, removedKey);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var target2 = await query.LoadAsync<Target>(target.Id);
        target2!.ChildrenDictionary.Count.ShouldBe(initialCount - 1);
        target2.ChildrenDictionary.ShouldNotContainKey(removedKey);
    }

    // ---- Delete operations ----

    [Fact]
    public async Task delete_redundant_property()
    {
        var target = Target.Random();
        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Delete("String");
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var result = await query.LoadAsync<Target>(target.Id);
        result!.String.ShouldBeNull();
    }

    [Fact]
    public async Task delete_redundant_nested_property()
    {
        var target = Target.Random(true);
        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Delete("String", t => t.Inner!);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var result = await query.LoadAsync<Target>(target.Id);
        result!.Inner!.String.ShouldBeNull();
    }

    [Fact]
    public async Task delete_existing_property()
    {
        var target = Target.Random(true);
        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Delete(t => t.Inner);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var result = await query.LoadAsync<Target>(target.Id);
        result!.Inner.ShouldBeNull();
    }

    // ---- Chaining operations ----

    [Fact]
    public async Task able_to_chain_patch_operations()
    {
        var target = Target.Random(true);
        target.Number = 5;

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id)
            .Set(x => x.Number, 10)
            .Increment(x => x.Number, 10);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Target>(target.Id))!.Number.ShouldBe(20);
    }

    // ---- Patch and Load ----

    [Fact]
    public async Task patch_and_load_should_return_updated_result()
    {
        var target = Target.Random();
        target.Number = 5;

        theSession.Store(target);
        await theSession.SaveChangesAsync();

        theSession.Patch<Target>(target.Id).Set(x => x.Number, 10);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<Target>(target.Id);
        loaded.ShouldNotBeNull();
        loaded.Number.ShouldBe(10);
    }
}
