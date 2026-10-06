package com.flowforge.orchestrator.testing;

import com.flowforge.orchestrator.api.CurrentUser;
import com.flowforge.orchestrator.application.AuthenticatedUser;
import com.flowforge.orchestrator.application.NotFoundException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * A dummy owned resource for T07.5 ("write this test against a dummy owned resource now"). It follows the
 * pattern real resources use: look up by id <em>and</em> owner, and treat "not yours" exactly like "not
 * there" (404), so ids of other users' resources can't be probed.
 */
@RestController
@RequestMapping("/api/v1/test/notes")
class OwnedNotesController {

    record Note(UUID id, UUID ownerId) {
    }

    private final Map<UUID, Note> notes = new ConcurrentHashMap<>();

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    Note create(@CurrentUser AuthenticatedUser user) {
        Note note = new Note(UUID.randomUUID(), user.id());
        notes.put(note.id(), note);
        return note;
    }

    @GetMapping("/{id}")
    Note get(@PathVariable UUID id, @CurrentUser AuthenticatedUser user) {
        Note note = notes.get(id);
        if (note == null || !note.ownerId().equals(user.id())) {
            throw new NotFoundException("note", id);
        }
        return note;
    }
}
