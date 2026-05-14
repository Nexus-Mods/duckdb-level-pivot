#include "level_pivot_overlay.hpp"

namespace level_pivot {

void TransactionOverlay::put(std::string_view key, std::string_view value) {
	entries_.insert_or_assign(std::string(key), std::optional<std::string>(std::string(value)));
}

void TransactionOverlay::del(std::string_view key) {
	entries_.insert_or_assign(std::string(key), std::nullopt);
}

void TransactionOverlay::clear() {
	entries_.clear();
}

TransactionOverlay::Lookup TransactionOverlay::lookup(std::string_view key, std::string_view *out_value) const {
	auto it = entries_.find(key);
	if (it == entries_.end()) {
		return Lookup::kAbsent;
	}
	// Bind to a local so clang-tidy can track has_value() across the access.
	// Reading it->second twice would be treated as two independent optionals.
	const auto &opt = it->second;
	if (!opt.has_value()) {
		return Lookup::kTombstone;
	}
	if (out_value) {
		const auto &v = *opt;
		*out_value = std::string_view(v.data(), v.size());
	}
	return Lookup::kPut;
}

MergedIterator::MergedIterator(LevelDBIterator base, const TransactionOverlay *overlay)
    : base_(std::move(base)), overlay_(overlay) {
	if (overlay_) {
		ov_it_ = overlay_->end();
	}
}

void MergedIterator::seek(std::string_view key) {
	base_.seek(key);
	if (overlay_) {
		ov_it_ = overlay_->lower_bound(key);
	}
	advance_to_next_visible();
}

void MergedIterator::seek_to_first() {
	base_.seek_to_first();
	if (overlay_) {
		ov_it_ = overlay_->begin();
	}
	advance_to_next_visible();
}

void MergedIterator::advance_to_next_visible() {
	while (true) {
		if (overlay_ && ov_it_ != overlay_->end() && !ov_it_->second.has_value()) {
			if (base_.valid() && base_.key_view() == std::string_view(ov_it_->first)) {
				base_.next();
			}
			++ov_it_;
			continue;
		}
		if (overlay_ && ov_it_ != overlay_->end() && base_.valid() &&
		    base_.key_view() == std::string_view(ov_it_->first)) {
			base_.next();
			continue;
		}
		break;
	}
	choose_state();
}

void MergedIterator::choose_state() {
	bool base_ok = base_.valid();
	bool ov_ok = overlay_ && ov_it_ != overlay_->end();
	if (!base_ok && !ov_ok) {
		state_ = State::kEnd;
		return;
	}
	if (!base_ok) {
		state_ = State::kOverlay;
		return;
	}
	if (!ov_ok) {
		state_ = State::kBase;
		return;
	}
	if (std::string_view(ov_it_->first) < base_.key_view()) {
		state_ = State::kOverlay;
	} else {
		state_ = State::kBase;
	}
}

void MergedIterator::next() {
	if (state_ == State::kBase) {
		base_.next();
	} else if (state_ == State::kOverlay) {
		++ov_it_;
	}
	advance_to_next_visible();
}

std::string_view MergedIterator::key_view() const {
	if (state_ == State::kOverlay) {
		return std::string_view(ov_it_->first);
	}
	return base_.key_view();
}

std::string_view MergedIterator::value_view() const {
	if (state_ == State::kOverlay) {
		// Invariant: kOverlay state implies a put (tombstones are skipped by
		// advance_to_next_visible). Bind to a local so clang-tidy tracks the
		// has_value() check across the dereference; the empty branch is
		// unreachable.
		const auto &opt = ov_it_->second;
		if (opt.has_value()) {
			const auto &str = *opt;
			return std::string_view(str.data(), str.size());
		}
		return std::string_view();
	}
	return base_.value_view();
}

} // namespace level_pivot
