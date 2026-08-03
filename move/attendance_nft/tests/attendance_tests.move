#[test_only]
module attendance_nft::attendance_tests {
    use sui::test_scenario::{Self, Scenario};
    use attendance_nft::attendance::{Self, MintCap, AttendanceNFT};

    const ADMIN: address = @0xA;
    const USER1: address = @0xB;
    const USER2: address = @0xC;

    #[test]
    fun test_mint_single_creates_nft() {
        let mut scenario = test_scenario::begin(ADMIN);
        let recipient = USER1;
        {
            let ctx = test_scenario::ctx(&mut scenario);
            let cap = attendance::create_test_cap(ctx);
            attendance::mint_attendance_nft(
                &cap, recipient,
                b"ride-001", 20260727,
                b"Space Mountain — 2026-07-27",
                b"ipfs://QmTest123/metadata.json",
                ctx,
            );
            attendance::destroy_test_cap(cap);
        };
        test_scenario::next_tx(&mut scenario, ADMIN);
        let nft = test_scenario::take_from_address<AttendanceNFT>(&scenario, recipient);
        assert!(attendance::get_recipient(&nft) == recipient, 0);
        assert!(attendance::get_ride_id(&nft) == b"ride-001", 1);
        assert!(attendance::get_date(&nft) == 20260727, 2);
        test_scenario::return_to_address(recipient, nft);
        test_scenario::end(scenario);
    }

    #[test]
    #[expected_failure(abort_code = 1, location = attendance_nft::attendance)]
    fun test_mint_rejects_zero_address() {
        let mut scenario = test_scenario::begin(ADMIN);
        {
            let ctx = test_scenario::ctx(&mut scenario);
            let cap = attendance::create_test_cap(ctx);
            attendance::mint_attendance_nft(
                &cap, @0x0,
                b"ride-001", 20260727,
                b"Test NFT",
                b"ipfs://QmTest/metadata.json",
                ctx,
            );
            attendance::destroy_test_cap(cap);
        };
        test_scenario::end(scenario);
    }

    #[test]
    #[expected_failure(abort_code = 2, location = attendance_nft::attendance)]
    fun test_mint_rejects_empty_name() {
        let mut scenario = test_scenario::begin(ADMIN);
        {
            let ctx = test_scenario::ctx(&mut scenario);
            let cap = attendance::create_test_cap(ctx);
            attendance::mint_attendance_nft(
                &cap, USER1,
                b"ride-001", 20260727,
                b"",
                b"ipfs://QmTest/metadata.json",
                ctx,
            );
            attendance::destroy_test_cap(cap);
        };
        test_scenario::end(scenario);
    }

    #[test]
    #[expected_failure(abort_code = 3, location = attendance_nft::attendance)]
    fun test_mint_rejects_empty_metadata_url() {
        let mut scenario = test_scenario::begin(ADMIN);
        {
            let ctx = test_scenario::ctx(&mut scenario);
            let cap = attendance::create_test_cap(ctx);
            attendance::mint_attendance_nft(
                &cap, USER1,
                b"ride-001", 20260727,
                b"Test NFT",
                b"",
                ctx,
            );
            attendance::destroy_test_cap(cap);
        };
        test_scenario::end(scenario);
    }

    #[test]
    fun test_batch_mint_creates_correct_count() {
        let mut scenario = test_scenario::begin(ADMIN);
        let recipient = USER1;
        {
            let ctx = test_scenario::ctx(&mut scenario);
            let cap = attendance::create_test_cap(ctx);
            attendance::mint_batch(
                &cap, recipient,
                vector[b"ride-001", b"ride-002", b"ride-003"],
                20260727,
                vector[b"Ride 1", b"Ride 2", b"Ride 3"],
                vector[b"ipfs://QmA", b"ipfs://QmB", b"ipfs://QmC"],
                ctx,
            );
            attendance::destroy_test_cap(cap);
        };
        test_scenario::next_tx(&mut scenario, ADMIN);
        let nft1 = test_scenario::take_from_address<AttendanceNFT>(&scenario, recipient);
        let nft2 = test_scenario::take_from_address<AttendanceNFT>(&scenario, recipient);
        let nft3 = test_scenario::take_from_address<AttendanceNFT>(&scenario, recipient);
        // take_from_address returns the most-recently-transferred object first (LIFO),
        // so nft1 corresponds to the last-minted ride (ride-003).
        assert!(attendance::get_ride_id(&nft1) == b"ride-003", 10);
        assert!(attendance::get_ride_id(&nft2) == b"ride-002", 11);
        assert!(attendance::get_ride_id(&nft3) == b"ride-001", 12);
        test_scenario::return_to_address(recipient, nft3);
        test_scenario::return_to_address(recipient, nft2);
        test_scenario::return_to_address(recipient, nft1);
        test_scenario::end(scenario);
    }

    #[test]
    #[expected_failure(abort_code = 4, location = attendance_nft::attendance)]
    fun test_batch_mint_mismatched_names_count() {
        let mut scenario = test_scenario::begin(ADMIN);
        {
            let ctx = test_scenario::ctx(&mut scenario);
            let cap = attendance::create_test_cap(ctx);
            attendance::mint_batch(
                &cap, USER1,
                vector[b"ride-001", b"ride-002"],
                20260727,
                vector[b"Only One Name"],
                vector[b"ipfs://QmA", b"ipfs://QmB"],
                ctx,
            );
            attendance::destroy_test_cap(cap);
        };
        test_scenario::end(scenario);
    }

    #[test]
    #[expected_failure(abort_code = 5, location = attendance_nft::attendance)]
    fun test_batch_mint_mismatched_urls_count() {
        let mut scenario = test_scenario::begin(ADMIN);
        {
            let ctx = test_scenario::ctx(&mut scenario);
            let cap = attendance::create_test_cap(ctx);
            attendance::mint_batch(
                &cap, USER1,
                vector[b"ride-001", b"ride-002"],
                20260727,
                vector[b"Name 1", b"Name 2"],
                vector[b"ipfs://QmA"],
                ctx,
            );
            attendance::destroy_test_cap(cap);
        };
        test_scenario::end(scenario);
    }

    #[test]
    #[expected_failure(abort_code = 1, location = attendance_nft::attendance)]
    fun test_batch_mint_rejects_zero_address() {
        let mut scenario = test_scenario::begin(ADMIN);
        {
            let ctx = test_scenario::ctx(&mut scenario);
            let cap = attendance::create_test_cap(ctx);
            attendance::mint_batch(
                &cap, @0x0,
                vector[b"ride-001"],
                20260727,
                vector[b"Test"],
                vector[b"ipfs://QmTest"],
                ctx,
            );
            attendance::destroy_test_cap(cap);
        };
        test_scenario::end(scenario);
    }

    #[test]
    fun test_burn_destroys_nft() {
        let mut scenario = test_scenario::begin(ADMIN);
        let recipient = USER1;
        {
            let ctx = test_scenario::ctx(&mut scenario);
            let cap = attendance::create_test_cap(ctx);
            attendance::mint_attendance_nft(
                &cap, recipient,
                b"ride-001", 20260727,
                b"Burnable NFT",
                b"ipfs://QmTest",
                ctx,
            );
            attendance::destroy_test_cap(cap);
        };
        test_scenario::next_tx(&mut scenario, ADMIN);
        let nft = test_scenario::take_from_address<AttendanceNFT>(&scenario, recipient);
        {
            let ctx = test_scenario::ctx(&mut scenario);
            let cap = attendance::create_test_cap(ctx);
            attendance::burn(&cap, nft);
            attendance::destroy_test_cap(cap);
        };
        test_scenario::end(scenario);
    }

    #[test]
    fun test_update_metadata_changes_url() {
        let mut scenario = test_scenario::begin(ADMIN);
        let recipient = USER1;
        {
            let ctx = test_scenario::ctx(&mut scenario);
            let cap = attendance::create_test_cap(ctx);
            attendance::mint_attendance_nft(
                &cap, recipient,
                b"ride-001", 20260727,
                b"Updatable NFT",
                b"ipfs://QmOriginal",
                ctx,
            );
            attendance::destroy_test_cap(cap);
        };
        test_scenario::next_tx(&mut scenario, ADMIN);
        let mut nft = test_scenario::take_from_address<AttendanceNFT>(&scenario, recipient);
        {
            let ctx = test_scenario::ctx(&mut scenario);
            let cap = attendance::create_test_cap(ctx);
            attendance::update_metadata(&cap, &mut nft, b"ipfs://QmUpdated");
            attendance::destroy_test_cap(cap);
        };
        let _ = attendance::get_metadata_url(&nft);
        test_scenario::return_to_address(recipient, nft);
        test_scenario::end(scenario);
    }

    #[test]
    fun test_view_functions() {
        let mut scenario = test_scenario::begin(ADMIN);
        let recipient = USER2;
        {
            let ctx = test_scenario::ctx(&mut scenario);
            let cap = attendance::create_test_cap(ctx);
            attendance::mint_attendance_nft(
                &cap, recipient,
                b"ride-005", 20260727,
                b"View Test NFT",
                b"ipfs://QmView",
                ctx,
            );
            attendance::destroy_test_cap(cap);
        };
        test_scenario::next_tx(&mut scenario, ADMIN);
        let nft = test_scenario::take_from_address<AttendanceNFT>(&scenario, recipient);
        assert!(attendance::get_recipient(&nft) == recipient, 30);
        assert!(attendance::get_ride_id(&nft) == b"ride-005", 31);
        assert!(attendance::get_date(&nft) == 20260727, 32);
        let _ = attendance::get_name(&nft);
        let _ = attendance::get_metadata_url(&nft);
        test_scenario::return_to_address(recipient, nft);
        test_scenario::end(scenario);
    }
}