package com.flowforge.orchestrator.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flowforge.core.error.ErrorCode;
import com.flowforge.core.error.FlowForgeException;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

class PageRequestFactoryTest {

    private final PageRequestFactory factory = new PageRequestFactory();
    private final PageRequestFactory.SortWhitelist whitelist =
        new PageRequestFactory.SortWhitelist(Map.of("createdAt", "createdAt", "name", "name"), "createdAt");

    @Test
    @DisplayName("T06.7 size 1..100 accepted; 0, 101 and a negative page rejected with VALIDATION_FAILED")
    void sizeAndPageBounds() {
        assertThat(factory.create(0, 100, null, whitelist).getPageSize()).isEqualTo(100);
        assertThat(factory.create(3, 1, null, whitelist).getPageNumber()).isEqualTo(3);

        for (int[] bad : new int[][] {{0, 0}, {0, 101}, {-1, 20}}) {
            assertThatThrownBy(() -> factory.create(bad[0], bad[1], null, whitelist))
                .isInstanceOfSatisfying(FlowForgeException.class, e ->
                    assertThat(e.errorCode()).isEqualTo(ErrorCode.VALIDATION_FAILED));
        }
    }

    @Test
    @DisplayName("T06.extra-7 a whitelist whose default field isn't whitelisted is rejected when it's built (not on a request)")
    void defaultMustBeWhitelisted() {
        assertThatThrownBy(() -> new PageRequestFactory.SortWhitelist(Map.of("name", "name"), "createdAt"))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("T06.7 sort is whitelisted per resource; default is the resource's default field, descending")
    void sortWhitelist() {
        Pageable byName = factory.create(0, 20, "name,asc", whitelist);
        assertThat(byName.getSort()).isEqualTo(Sort.by(Sort.Order.asc("name")));

        Pageable byDefault = factory.create(0, 20, null, whitelist);
        assertThat(byDefault.getSort()).isEqualTo(Sort.by(Sort.Order.desc("createdAt")));

        Pageable directionOmitted = factory.create(0, 20, "createdAt", whitelist);
        assertThat(directionOmitted.getSort()).isEqualTo(Sort.by(Sort.Order.asc("createdAt")));

        for (String bad : new String[] {"password,asc", "name,sideways", "name,asc,extra", ","}) {
            assertThatThrownBy(() -> factory.create(0, 20, bad, whitelist))
                .isInstanceOfSatisfying(FlowForgeException.class, e ->
                    assertThat(e.errorCode()).isEqualTo(ErrorCode.INVALID_SORT));
        }
    }
}
