/// Module: attendance_nft::attendance
/// Mints transferable Attendance NFTs with IPFS metadata for verified ride scans.
/// Supports batch minting, admin-only metadata updates, and burning.
module attendance_nft::attendance {
    use sui::tx_context::{Self, TxContext};
    use sui::object::{Self, UID};
    use sui::transfer;
    use sui::url::{Self, Url};
    use sui::event;
    use std::string::{Self, String};

    /// Capability that grants the right to mint and manage Attendance NFTs.
    public struct MintCap has key {
        id: UID
    }

    /// An attendance NFT — transferable, represents one ride on one day.
    public struct AttendanceNFT has key, store {
        id: UID,
        recipient: address,
        ride_id: vector<u8>,
        date: u64,
        name: String,
        metadata_url: Url,
    }

    public struct AttendanceMinted has copy, drop {
        recipient: address,
        ride_ids: vector<vector<u8>>,
        date: u64,
    }

    public struct AttendanceBurned has copy, drop {
        nft_id: address,
        ride_id: vector<u8>,
        date: u64,
    }

    /// Test-only: create a MintCap for unit test isolation.
    #[test_only]
    public(package) fun create_test_cap(ctx: &mut TxContext): MintCap {
        MintCap { id: object::new(ctx) }
    }

    /// Test-only: destroy a MintCap after use in unit tests.
    #[test_only]
    public(package) fun destroy_test_cap(cap: MintCap) {
        let MintCap { id } = cap;
        object::delete(id);
    }

    /// Initialize the mint capability. Called once at deploy time.
    fun init(ctx: &mut TxContext) {
        let cap = MintCap { id: object::new(ctx) };
        transfer::transfer(cap, tx_context::sender(ctx));
    }

    /// Mint a single Attendance NFT. Requires MintCap.
    public fun mint_attendance_nft(
        _cap: &MintCap,
        recipient: address,
        ride_id: vector<u8>,
        date: u64,
        name: vector<u8>,
        metadata_url: vector<u8>,
        ctx: &mut TxContext
    ) {
        assert!(recipient != @0x0, 1);
        assert!(vector::length(&name) > 0, 2);
        assert!(vector::length(&metadata_url) > 0, 3);

        let nft = AttendanceNFT {
            id: object::new(ctx),
            recipient,
            ride_id,
            date,
            name: string::utf8(name),
            metadata_url: url::new_unsafe_from_bytes(metadata_url),
        };
        transfer::public_transfer(nft, recipient);
        event::emit(AttendanceMinted {
            recipient,
            ride_ids: vector[ride_id],
            date,
        });
    }

    /// Batch mint: multiple ride_ids in a single transaction.
    public fun mint_batch(
        _cap: &MintCap,
        recipient: address,
        ride_ids: vector<vector<u8>>,
        date: u64,
        names: vector<vector<u8>>,
        metadata_urls: vector<vector<u8>>,
        ctx: &mut TxContext
    ) {
        assert!(recipient != @0x0, 1);
        let count = vector::length(&ride_ids);
        assert!(vector::length(&names) == count, 4);
        assert!(vector::length(&metadata_urls) == count, 5);

        let mut i = 0;
        while (i < count) {
            let ride_id = *vector::borrow(&ride_ids, i);
            let name_bytes = *vector::borrow(&names, i);
            let metadata_url_bytes = *vector::borrow(&metadata_urls, i);

            let nft = AttendanceNFT {
                id: object::new(ctx),
                recipient,
                ride_id,
                date,
                name: string::utf8(name_bytes),
                metadata_url: url::new_unsafe_from_bytes(metadata_url_bytes),
            };
            transfer::public_transfer(nft, recipient);
            i = i + 1;
        };
        event::emit(AttendanceMinted {
            recipient,
            ride_ids,
            date,
        });
    }

    /// Burn an NFT. Requires MintCap (admin only).
    public fun burn(_cap: &MintCap, nft: AttendanceNFT) {
        let AttendanceNFT {
            id,
            recipient: _,
            ride_id,
            date,
            name: _,
            metadata_url: _,
        } = nft;

        event::emit(AttendanceBurned {
            nft_id: object::uid_to_address(&id),
            ride_id,
            date,
        });
        object::delete(id);
    }

    /// Update the metadata_url of an existing NFT. Admin only.
    public fun update_metadata(_cap: &MintCap, nft: &mut AttendanceNFT, new_metadata_url: vector<u8>) {
        nft.metadata_url = url::new_unsafe_from_bytes(new_metadata_url);
    }

    // View functions
    public fun get_recipient(nft: &AttendanceNFT): address { nft.recipient }
    public fun get_ride_id(nft: &AttendanceNFT): vector<u8> { nft.ride_id }
    public fun get_date(nft: &AttendanceNFT): u64 { nft.date }
    public fun get_name(nft: &AttendanceNFT): String { nft.name }
    public fun get_metadata_url(nft: &AttendanceNFT): Url { nft.metadata_url }
}