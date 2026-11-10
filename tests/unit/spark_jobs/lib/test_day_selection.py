"""Test cho `spark_jobs/lib/day_selection.py` — cách chọn tập ngày export để nạp thử.

Vì sao test phần này: nó quyết định ảnh Spark UI có lộ skew hay không. Lấy ngày liền nhau
ở cuối dải thì mọi task to gần bằng nhau và bằng chứng skew biến mất, nên "rải đều" là
hành vi phải khoá lại bằng test, không phải chi tiết tuỳ ý.
"""

from __future__ import annotations

import pytest

from spark_jobs.lib.day_selection import pick_sample_days, pick_window_days

# 180 ngày giống thật: 2026-01-01 .. 2026-06-29.
DAYS = [f"d{i:03d}" for i in range(180)]


def test_lay_het_khi_n_bang_0_hoac_qua_lon():
    """n <= 0 và n >= số ngày đều nghĩa là 'nạp hết', trả về chính danh sách đầu vào."""
    assert pick_sample_days(DAYS, 0) == DAYS
    assert pick_sample_days(DAYS, -5) == DAYS
    assert pick_sample_days(DAYS, 180) == DAYS
    assert pick_sample_days(DAYS, 500) == DAYS


def test_dung_so_ngay_va_khong_trung_lap():
    """Lấy n ngày thì phải ra đúng n ngày khác nhau, giữ nguyên thứ tự tăng."""
    picked = pick_sample_days(DAYS, 20)
    assert len(picked) == 20
    assert len(set(picked)) == 20
    assert picked == sorted(picked)


def test_luon_gom_ngay_dau_va_ngay_cuoi():
    """Đây là điều kiện để skew lộ ra: ngày đầu dải nhỏ nhất, ngày cuối lớn nhất."""
    for n in (2, 3, 7, 20, 179):
        picked = pick_sample_days(DAYS, n)
        assert picked[0] == DAYS[0], n
        assert picked[-1] == DAYS[-1], n


def test_rai_deu_chu_khong_don_ve_mot_dau():
    """Khoảng cách giữa các ngày được chọn phải xấp xỉ nhau, lệch nhiều nhất 1 bậc.

    Nếu hàm đổi thành 'n ngày cuối' thì test này vỡ: mọi khoảng cách sẽ là 1 trừ một
    khoảng khổng lồ ở đầu.
    """
    picked = pick_sample_days(DAYS, 20)
    idx = [DAYS.index(d) for d in picked]
    gaps = [b - a for a, b in zip(idx, idx[1:])]
    assert max(gaps) - min(gaps) <= 1, gaps


def test_n_bang_1_lay_ngay_lon_nhat():
    """Chỉ một ngày thì lấy ngày cuối, vì đó là ngày nhiều dữ liệu nhất."""
    assert pick_sample_days(DAYS, 1) == [DAYS[-1]]


@pytest.mark.parametrize("n", [2, 5, 50])
def test_moi_ngay_duoc_chon_la_ngay_co_that(n):
    """Hàm chỉ được chọn lại từ danh sách đầu vào, không tự sinh ngày mới."""
    assert set(pick_sample_days(DAYS, n)) <= set(DAYS)


class TestPickWindowDays:
    """Chọn ngày LIỀN NHAU — mục đích ngược với `pick_sample_days`.

    Bản trùng của generator nằm trong khoảng `overlap_days = 3`, nên hai ngày cách nhau
    9-10 ngày không bao giờ chứa cùng một Order. Đã đo: 20 ngày rải đều cho dedup bắt được
    đúng 0 bản trùng (`docs/data-platform/reference/silver.md` mục 10.3). Vì vậy phải có cách chọn ngày liền nhau.
    """

    DAYS = [f"2026-01-{d:02d}" for d in range(1, 21)]

    def test_lien_nhau_tu_ngay_chi_dinh(self):
        """Lấy `n` ngày liền nhau bắt đầu từ `start`."""
        got = pick_window_days(self.DAYS, 3, start="2026-01-05")
        assert got == ["2026-01-05", "2026-01-06", "2026-01-07"]

    def test_khong_chi_dinh_thi_lay_GIUA_dai(self):
        """Lấy giữa vì hai đầu dải là ca biên: đầu ngày quá bé, cuối ngày quá to."""
        got = pick_window_days(self.DAYS, 4)
        assert got == ["2026-01-09", "2026-01-10", "2026-01-11", "2026-01-12"]

    def test_cac_ngay_tra_ve_LIEN_TUC_khong_ho(self):
        """Các ngày trả về liền nhau, không rải như `pick_sample_days`."""
        got = pick_window_days(self.DAYS, 7, start="2026-01-02")
        idx = [self.DAYS.index(d) for d in got]
        assert idx == list(range(idx[0], idx[0] + 7))

    def test_cham_cuoi_dai_thi_lui_lai_cho_du_n(self):
        """Yêu cầu 5 ngày từ ngày thứ 18 chỉ còn 3 ngày — phải lùi để vẫn đủ 5."""
        got = pick_window_days(self.DAYS, 5, start="2026-01-18")
        assert len(got) == 5
        assert got[-1] == "2026-01-20"

    def test_n_lon_hon_ca_dai_thi_lay_het(self):
        """`n` lớn hơn cả dải thì lấy hết."""
        assert pick_window_days(self.DAYS, 99) == self.DAYS

    def test_n_bang_khong_thi_rong(self):
        """`n = 0` thì trả rỗng."""
        assert pick_window_days(self.DAYS, 0) == []

    def test_ngay_bat_dau_sai_thi_NEM_LOI(self):
        """Ném chứ không lặng lẽ chọn ngày gần nhất.

        Gõ sai ngày rồi chạy 20 phút ra bảng không như mong đợi thì tệ hơn là dừng ngay.
        """
        with pytest.raises(ValueError, match="không có trong danh sách"):
            pick_window_days(self.DAYS, 3, start="2026-99-99")

    def test_khac_han_pick_sample_days_tren_cung_dau_vao(self):
        """Hai hàm cùng n mà ra hai tập khác nhau — đó chính là lý do có hai hàm."""
        n = 5
        assert pick_window_days(self.DAYS, n) != pick_sample_days(self.DAYS, n)
